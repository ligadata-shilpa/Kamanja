/*
 * Copyright 2015 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ligadata.jpmml

import java.io.{PushbackInputStream, ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.{List => JList}
import javax.xml.bind.{ValidationEvent, ValidationEventHandler}
import javax.xml.transform.sax.SAXSource

import com.ligadata.KamanjaBase._
import com.ligadata.kamanja.metadata._

import org.apache.logging.log4j.LogManager

import org.dmg.pmml.{PMML, FieldName}
import org.jpmml.evaluator._
import org.jpmml.model.{JAXBUtil, ImportFilter}

import org.xml.sax.InputSource
import org.xml.sax.helpers.XMLReaderFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.concurrent.TrieMap

object GlobalLogger {
    val loggerName = this.getClass.getName()
    val logger = LogManager.getLogger(loggerName)
}

trait LogTrait {
    val logger = GlobalLogger.logger
}

/**
 * JpmmlAdapter serves a "shim" between the engine and a JPMML evaluator that will perform the actual message
 * scoring. It exhibits the "adapter" pattern as discussed in "Design Patterns" by Gamma, Helm, Johnson, and Vlissitudes.
 *
 * Kamanja messages are presented to the adapter and transformed to a Map[FieldName, FieldValue] for consumption by
 * the JPMML evaluator associated with the JpmmlAdapter instance. The target fields (or predictions) and the output fields
 * are returned in the MappedModelResults instance to the engine for further transformation and dispatch.
 *
 * There is a partition hash key associated with the model base. The JPmmlAdapter object will set this initially to 0.  The
 * engine will modify it (it's a var) if appropriate.
 *
 * @param factory This model's factory object
 * @param modelEvaluator The JPMML evaluator needed to evaluate the model for this model (modelName.modelVersion)
 */

class JpmmlAdapter(factory : ModelInstanceFactory, modelEvaluator: ModelEvaluator[_]) extends ModelInstance(factory) {

    /**
     * The engine will call this method to have the model evaluate the input message and produce a ModelResultBase with results.
     * @param txnCtxt the transaction context (contains message, transaction id, partition key, raw data, et al)
     * @param outputDefault when true, a model result will always be produced with default values.  If false (ordinary case), output is
     *                      emitted only when the model deems this message worthy of report.  If desired the model may return a 'null'
     *                      for the execute's return value and the engine will not proceed with output processing
     * @return a ModelResultBase derivative or null (if there is nothing to report and outputDefault is false).
     */
    override def execute(txnCtxt: TransactionContext, outputDefault: Boolean): ModelResultBase = {
        val msg = txnCtxt.getMessage()
        evaluateModel(msg)
    }

    /**
     * Prepare the active fields, call the model evaluator, and emit results.
     * @param msg the incoming message containing the values of interest to be assigned to the active fields in the
     *            model's data dictionary.
     * @return
     */
    private def evaluateModel(msg : MessageContainerBase): ModelResultBase = {
        val activeFields = modelEvaluator.getActiveFields
        val preparedFields = prepareFields(activeFields, msg, modelEvaluator)
        val evalResultRaw : MutableMap[FieldName, _] = modelEvaluator.evaluate(preparedFields.asJava).asScala
        val evalResults = replaceNull(evalResultRaw)
        val results = EvaluatorUtil.decode(evalResults.asJava).asScala
        new MappedModelResults().withResults(results.toArray)
    }


    /** Since the JPMML decode util for the map results shows that at least one key can possibly be null,
      * let's give a name to it for our results.  This is likely just careful coding, but no harm
      * taking precaution.  This is the tag for the null field:
      */
    val DEFAULT_NAME : FieldName = FieldName.create("Anon_Result")

    /**
     * If there is a missing key (an anonymous result), try to manufacture a key for the map so that 
     * all of the result fields returned are decoded and returned.
     * 
     * @param evalResults a mutable map of the results from an evaluator's evaluate function
     * @tparam V the result type
     * @return a map with any null key replaced with a manufactured field name
     */
    private def replaceNull[V](evalResults: MutableMap[FieldName, V]): MutableMap[FieldName, V] = {
         evalResults.get(null.asInstanceOf[FieldName]).fold(evalResults)(v => {
             evalResults - null.asInstanceOf[FieldName] + (DEFAULT_NAME -> v)
        })
    }

    /**
     * Prepare the active fields in the data dictionary of the model with corresponding values from the incoming
     * message.  Note that currently all field names in the model's data dictionary must exactly match the field names
     * from the message.  There is no mapping capability metadata at this point.
     *
     * NOTE: It is possible to have missing inputs in the message.  The model, if written robustly, has accounted
     * for missingValue and other strategies needed to produce results even with imperfect inputs.
     * @see http://dmg.org/pmml/v4-2-1/MiningSchema.html for a discussion about this.
     *
     * @param activeFields a List of the FieldNames
     * @param msg the incoming message instance
     * @param evaluator the JPMML evaluator that the factory as arranged for this instance that can process the
     *                  input values.
     * @return
     */
    private def prepareFields(activeFields: JList[FieldName]
                              , msg: MessageContainerBase
                              , evaluator: ModelEvaluator[_]) : Map[FieldName, FieldValue] = {
        activeFields.asScala.foldLeft(Map.empty[FieldName, FieldValue])((map, activeField) => {
            val key = activeField.getValue
            Option(msg.get(key)).fold(map)(value => {
                val fieldValue : FieldValue = EvaluatorUtil.prepare(evaluator, activeField, value)
                map.updated(activeField, fieldValue)

            })
        })
    }
}



/**
  * The JpmmlAdapterFactory instantiates its JPMML model instance when asked by caller.  Its main function is to
  * instantiate a new model whenever asked (createModelInstance) and assess whether the current message being processed
  * by the engine is consumable by this model (isValidMessage)
  *
  * @param modelDef the model definition that describes the model that this factory will prepare
  * @param nodeContext the NodeContext object can be used by the model instances to put/get model state needed to
  *                    execute the model.
  */

// ModelInstanceFactory will be created from FactoryOfModelInstanceFactory when metadata got resolved (while engine is starting and when metadata adding while running the engine).
class JpmmlAdapterFactory(modelDef: ModelDef, nodeContext: NodeContext) extends ModelInstanceFactory(modelDef, nodeContext) with LogTrait {

    // This calls when the instance got created. And only calls once per instance.
    // Common initialization for all Model Instances. This gets called once per node during the metadata load or corresponding model def change.
    //	Intput:
    //		txnCtxt: Transaction context to do get operations on this transactionid. But this transaction will be rolledback once the initialization is done.
    override def init(txnContext: TransactionContext): Unit = {}

    // This calls when the factory is shutting down. There is no guarantee.
    override def shutdown(): Unit = {} // Shutting down this factory.

    /**
      * Answer the model name.
      * @return the model namespace.name.version
      */
    override def getModelName: String = {
        val name : String = if (getModelDef() != null) {
            getModelDef.FullNameWithVer
        } else {
            val msg : String =  "JpmmlAdapterFactory: model has no name and no version"
            logger.error(msg)
            msg
        }
        name
    }

    /**
      * Answer the model version.
      * @return the model version
      */
    override def getVersion: String = {
        val withDots: Boolean = true
        if (modelDef != null) {
            MdMgr.ConvertLongVersionToString(modelDef.Version, withDots)
        } else {
            if (withDots) "000000.000001.000000" else "000000000001000000"
        }
    }

    /**
      * Determine if the supplied message can be consumed by the model mentioned in the argument list.  The engine will
      * call this method when a new messages has arrived and been prepared.  It is passed to each of the active models
      * in the working set.  Each model has the opportunity to indicate its interest in the message.
      *
      * @param msg  - the message instance that is currently being processed
      * @return true if this model can process the message.
      */
    override def isValidMessage(msg: MessageContainerBase): Boolean = {
        val msgFullName : String = msg.FullName
        val msgVersion : String = msg.Version
        val msgNameKey : String = s"$msgFullName.$msgVersion"
        val yum : Boolean = if (modelDef != null) {
            (msgNameKey == modelDef.msgConsumed)
        } else {
            false
        }
        yum
    }

    /**
      * Answer a model instance, obtaining a pre-existing one in the cache if possible.
      * @return - a ModelInstance that can process the message found in the TransactionContext supplied at execution time
      */
    override def createModelInstance: ModelInstance = {

        val useThisModel : ModelInstance = if (modelDef != null) {
            /** Ingest the pmml here and build an evaluator */
            val modelEvaluator: ModelEvaluator[_] = createEvaluator(modelDef.jpmmlText)
            val isInstanceReusable : Boolean = true
            val builtModel : ModelInstance = new JpmmlAdapter( this, modelEvaluator)
            builtModel
        } else {
            logger.error("ModelDef in ctor was null...instance could not be built")
            null
        }
        useThisModel
    }

    /**
      * Create the appropriate JPMML evaluator based upon the pmml text supplied.
      *
      * @param pmmlText the PMML (xml) text for a JPMML consumable model
      * @return the appropriate JPMML ModelEvaluator
      */
    private def createEvaluator(pmmlText : String) : ModelEvaluator[_] = {

        val inputStream: InputStream = new ByteArrayInputStream(pmmlText.getBytes(StandardCharsets.UTF_8))
        val is = new PushbackInputStream(inputStream)

        val reader = XMLReaderFactory.createXMLReader()
        reader.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
        val filter = new ImportFilter(reader)
        val source = new SAXSource(filter, new InputSource(is))
        val unmarshaller = JAXBUtil.createUnmarshaller
        unmarshaller.setEventHandler(SimpleValidationEventHandler)

        val pmml: PMML = unmarshaller.unmarshal(source).asInstanceOf[PMML]
        val modelEvaluatorFactory = ModelEvaluatorFactory.newInstance()
        val modelEvaluator = modelEvaluatorFactory.newModelManager(pmml)
        modelEvaluator
    }


    /**
      * Answer the model name.
      * @return the model name
      */
    def ModelName(): String = {
        if (modelDef != null) modelDef.FullNameWithVer else "UNKNOWN MODEL NAME"
    }

    /**
      * Answer a ModelResultBase from which to give the model results.
      * @return - a ModelResultBase derivative appropriate for the model
      */
    override def createResultObject(): ModelResultBase = new MappedModelResults

    /**
      *  Is the ModelInstance created by this ModelInstanceFactory is reusable? NOTE: All JPmml models are resusable.
      *
      *  @return true
      */
    override def isModelInstanceReusable: Boolean = true

    /**
      * Standard handler fed to the unmarshaller to handle error conditions.
      */
    private object SimpleValidationEventHandler extends ValidationEventHandler {
        def handleEvent(event: ValidationEvent): Boolean = {
            val severity: Int = event.getSeverity
            severity match {
                case ValidationEvent.ERROR => false
                case ValidationEvent.FATAL_ERROR => false
                case _ => true
            }
        }
    }

}




