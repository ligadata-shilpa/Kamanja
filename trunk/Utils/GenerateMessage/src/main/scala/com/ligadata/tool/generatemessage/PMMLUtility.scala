package com.ligadata.tool.generatemessage

/**
  * Created by Yousef on 5/24/2016.
  */

//import com.ligadata.KamanjaBase.FactoryOfModelInstanceFactory
//import com.ligadata.kamanja.metadata.MiningModelType._

//import com.ligadata.Serialize._
import org.dmg.pmml._

import scala.collection.JavaConverters._

import java.io.{ByteArrayInputStream, PushbackInputStream, InputStream}
import java.nio.charset.StandardCharsets
import javax.xml.bind.{ValidationEvent, ValidationEventHandler}
import javax.xml.transform.sax.SAXSource
import java.util.{List => JList}

//import com.ligadata.kamanja.metadata._
//import com.ligadata.jpmml.JpmmlAdapter
import org.jpmml.model.{JAXBUtil, ImportFilter}
import org.jpmml.evaluator._
import org.xml.sax.InputSource
import org.xml.sax.helpers.XMLReaderFactory

class PMMLUtility extends LogTrait{
  def XMLReader(pmmlText: String): ModelEvaluator[_ <: Model]={
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

    if (modelEvaluator == null){
      logger.error("no fields found in pmml file")
      sys.exit(1)
    }

    return modelEvaluator
    //if (modelEvaluator != null) {
    //  val activeFieldNames: JList[FieldName] = modelEvaluator.getActiveFields
    //  val outputFieldNames: JList[FieldName] = modelEvaluator.getOutputFields
//      val targetFieldNames: JList[FieldName] = modelEvaluator.getTargetFields
//
//      val activeFields: scala.Array[DataField] = {
//        activeFieldNames.asScala.map(nm => modelEvaluator.getDataField(nm))
//      }.toArray
//      val targetFields: scala.Array[Target] = {
//        targetFieldNames.asScala.filter(nm => modelEvaluator.getTarget(nm) != null).map(nm => modelEvaluator.getTarget(nm))
//      }.toArray
//      val outputFields: scala.Array[OutputField] = {
//        outputFieldNames.asScala.filter(nm => modelEvaluator.getOutputField(nm) != null).map(nm => modelEvaluator.getOutputField(nm))
//      }.toArray

//      val activeFieldContent : scala.Array[(String,String)] = activeFields.map(fld => {
//        (fld.getName.getValue, fld.getDataType.value)
//      })

//      val targetFieldContent : scala.Array[(String,String)] = if (targetFields != null && targetFields.size > 0) {
//        targetFields.map(fld => {
//          val field : FieldName = fld.getField
//          val name : String = field.getValue
//          val datafield : DataField =  modelEvaluator.getDataField(field)
//          (datafield.getName.getValue, datafield.getDataType.value)
//
//        })
//      } else {
//        scala.Array[(String,String)](("no","targetFields"))
//      }

//      val outputFieldContent : scala.Array[(String,String)] = if (outputFields != null && outputFields.size > 0) {
//        outputFields.map(fld => {
//          (fld.getName.getValue, fld.getDataType.value)
//        })
//      } else {
//        scala.Array[(String,String)](("no","outputFields"))
//      }
//    }
  }

  def ActiveFields(modelEvaluator: ModelEvaluator[_ <: Model]): scala.Array[(String,String)] ={
    val activeFieldNames: JList[FieldName] = modelEvaluator.getActiveFields
    val activeFields: scala.Array[DataField] = {
      activeFieldNames.asScala.map(nm => modelEvaluator.getDataField(nm))
    }.toArray
    val activeFieldContent : scala.Array[(String,String)] = activeFields.map(fld => {
      (fld.getName.getValue, fld.getDataType.value)
    })
    return activeFieldContent
  }

  def OutputFields(modelEvaluator: ModelEvaluator[_ <: Model]): scala.Array[(String,String)] ={
    val outputFieldNames: JList[FieldName] = modelEvaluator.getOutputFields
    val outputFields: scala.Array[OutputField] = {
      outputFieldNames.asScala.filter(nm => modelEvaluator.getOutputField(nm) != null).map(nm => modelEvaluator.getOutputField(nm))
    }.toArray
    val outputFieldContent : scala.Array[(String,String)] = if (outputFields != null && outputFields.size > 0) {
      outputFields.map(fld => { if(fld.getDataType != null) {
        (fld.getName.getValue, fld.getDataType.value)
      }else (fld.getName.getValue, "String")
      })
    } else {
      //scala.Array[(String,String)](("no","outputFields"))
      scala.Array[(String,String)]()
    }

    return outputFieldContent
  }

  def TargetFields(modelEvaluator: ModelEvaluator[_ <: Model]): scala.Array[(String,String)] ={
    val targetFieldNames: JList[FieldName] = modelEvaluator.getTargetFields
    val targetFields: scala.Array[Target] = {
      targetFieldNames.asScala.filter(nm => modelEvaluator.getTarget(nm) != null).map(nm => modelEvaluator.getTarget(nm))
    }.toArray
    val targetFieldContent : scala.Array[(String,String)] = if (targetFields != null && targetFields.size > 0) {
      targetFields.map(fld => {
        val field : FieldName = fld.getField
        val name : String = field.getValue
        val datafield : DataField =  modelEvaluator.getDataField(field)
        if(datafield.getDataType != null){
        (datafield.getName.getValue, datafield.getDataType.value)
        } else (datafield.getName.getValue,"String")

      })
    } else {
      //scala.Array[(String,String)](("no","targetFields"))
      scala.Array[(String,String)]()
    }
    return targetFieldContent
  }

  def OutputMessageFields(outputFields: scala.Array[(String,String)], targetFields: scala.Array[(String,String)]): scala.Array[(String,String)] ={
    if(outputFields.length == 0 && targetFields.length == 0)
      return scala.Array[(String,String)]()
    else if(outputFields.length == 0 && targetFields.length != 0)
      return targetFields
    else if(outputFields.length != 0 && targetFields.length == 0)
      return outputFields
    else return outputFields ++ targetFields
  }
}

private object SimpleValidationEventHandler extends ValidationEventHandler {
  /**
    * Answer false whenever the validation event severity is ERROR or FATAL_ERROR.
    *
    * @param event a ValidationEvent issued by the JAXB SAX utility that is parsing the PMML source text.
    * @return flag to indicate whether to continue with the parse or not.
    */
  def handleEvent(event: ValidationEvent): Boolean = {
    val severity: Int = event.getSeverity
    severity match {
      case ValidationEvent.ERROR => false
      case ValidationEvent.FATAL_ERROR => false
      case _ => true
    }
  }
}
