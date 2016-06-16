/*
 * Copyright 2016 ligaDATA
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
package com.ligadata.samples.models

import org.python.core.Py
import org.python.core.PyObject
import org.python.core.PyString
import org.python.util.PythonInterpreter
import com.ligadata.kamanja.metadata.ModelDef
import com.ligadata.KamanjaBase._
import com.ligadata.kamanja.samples.messages._
import java.util.Properties
import com.ligadata.runtime.Log

class HelloWorldJythonFactory(modelDef: ModelDef, nodeContext: NodeContext) extends ModelInstanceFactory(modelDef, nodeContext) {

  override def getModelName(): String = {
    "com.ligadata.kamanja.samples.models.HelloWorldJythonModel"
  }

  override def getVersion(): String = {
    "0.0.8" // ToDo: get from metadata
  }

  override def isValidMessage(msg: MessageContainerBase): Boolean = {
    true
  }
  override def createModelInstance(): ModelInstance = {
    return new HelloWorldJythonModel(this)
  }
}

class HelloWorldJythonModel(factory: ModelInstanceFactory) extends ModelInstance(factory) {

  val code =
    """
      |#
      |# Copyright 2016 ligaDATA
      |#
      |# Licensed under the Apache License, Version 2.0 (the "License");
      |# you may not use this file except in compliance with the License.
      |# You may obtain a copy of the License at
      |#
      |#     http://www.apache.org/licenses/LICENSE-2.0
      |#
      |# Unless required by applicable law or agreed to in writing, software
      |# distributed under the License is distributed on an "AS IS" BASIS,
      |# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
      |# See the License for the specific language governing permissions and
      |# limitations under the License.
      |#
      |from com.ligadata.KamanjaBase import ContainerOrConcept
      |from com.ligadata.kamanja.samples.messages import msg1
      |from com.ligadata.kamanja.samples.messages import outmsg1
      |
      |class Model():
      |   def __init__(self):
      |
      |   def execute(self, txnCtxt, execMsgsSet, matchedInputSetIndex, outputDefault):
      |       inMsg = execMsgsSet[0]
      |       if inMsg.id()!=111:
      |           v = inMsg.in1()
      |           return None
      |
      |       output = outmsg1.createInstance()
      |       output.set(0, inMsg.id())
      |       output.set(1, inMsg.name())
      |
      |       return output
      |return Model()
    """.stripMargin
  val cp="/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/kvinit_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/containersutility_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/metadataapi_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/kamanjamanager_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/simplekafkaproducer_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/extractdata_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/jdbcdatacollector_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/metadataapiservice_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/filedataconsumer_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/cleanutil_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/pmmltesttool-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/jsonchecker_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/nodeinfoextract_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/ExtDependencyLibs_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/ExtDependencyLibs2_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/KamanjaInternalDeps_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/jarfactoryofmodelinstancefactory_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/migratebase-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/migratefrom_v_1_1_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/migratefrom_v_1_2_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/migratefrom_v_1_3_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/migrateto_v_1_4_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/generateadapterbindings_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/migratefrom_v_1_3_2.11-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.kamanja.samples.messages_msg1_1000000_1465412866388.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.kamanja.samples.messages_msg1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.kamanja.samples.messages_outmsg1_1000000_1465412910148.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.kamanja.samples.messages_outmsg1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V1_HelloWorlJythondModel_0.0.1_1465414330598.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V1_HelloWorlJythondModel_0.0.1_1465418910326.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V1_HelloWorlJythondModel_0.0.1_1465419011856.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V2_HelloWorlJythondModel_0.0.2_1465419034006.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V2_HelloWorlJythondModel_0.0.2_1465419074737.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V2_HelloWorlJythondModel_0.0.2_1465419108181.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V2_HelloWorlJythondModel_0.0.2_1465419132294.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V1_HelloWorlJythondModel_0.0.1_1465419169540.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V2_HelloWorlJythondModel_0.0.2_1465419205063.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V2_HelloWorlJythondModel_0.0.2_1465419564109.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465420419220.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465497260337.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465497469163.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465497494877.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465497511191.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465497535776.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465497619523.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465497692886.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465497779545.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V4_HelloWorlJythondModel_0.0.4_1465498567008.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V5_HelloWorlJythondModel_0.0.5_1465498923845.jar"

  var props: Properties = new Properties();
  props.put("python.home", "/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system")
  props.put("python.console.encoding", "UTF-8")
  props.put("python.security.respectJavaAccessibility", "false")
  props.put("python.import.site", "false");

  var preprops: Properties = System.getProperties();

  PythonInterpreter.initialize(preprops, props, Array.empty[String]);

  // Create the jython object
  val logger = new Log("com.ligadata.samples.models.HelloWorldJythonModel")
  logger.Info("<<< 10")
  val interpreter = new PythonInterpreter()
  logger.Info("<<< 11")

  def urlses(cl: ClassLoader): Array[java.net.URL] = cl match {
    case null => Array()
    case u: java.net.URLClassLoader => u.getURLs() ++ urlses(cl.getParent)
    case _ => urlses(cl.getParent)
  }

  val cl1 = interpreter.getSystemState().getClassLoader()
  val urls1 = urlses(cl1)
  logger.Info("CLASSPATH-JYTHON:=" + urls1.mkString(":"))

  //  val cl2 = java.lang.ClassLoader.getSystemClassLoader()
  //  val urls2 = urlses(cl2)
  //  logger.Info("CLASSPATH-JAVA:=" + urls2.mkString(":"))

  interpreter.getSystemState().setClassLoader(java.lang.ClassLoader.getSystemClassLoader())

  val cl3 = interpreter.getSystemState().getClassLoader()
  val urls3 = urlses(cl3)
  logger.Info("CLASSPATH-JYTHON-NEW:=" + urls3.mkString(":"))

  val modelObject: PyObject = try {
    interpreter.eval(code)
    logger.Info("<<< 12")
    // interpreter.exec("import Model")
    logger.Info("<<< 13")
    val modelClass = interpreter.get("Model")
    logger.Info("<<< 14")
    val modelObject: PyObject = modelClass.__call__()
    logger.Info("<<< 15")
    modelObject
  } catch {
    case e: Exception => println(e.toString)
      throw e
  }

  override def execute(txnCtxt: TransactionContext, execMsgsSet: Array[ContainerOrConcept], triggerdSetIndex: Int, outputDefault: Boolean): Array[ContainerOrConcept] = {
    logger.Info("<<< 20")
    val r: PyObject = modelObject.invoke("Execute", Array(Py.java2py(txnCtxt), Py.java2py(execMsgsSet), Py.java2py(0), Py.java2py(false)))
    logger.Info("<<< 21")
    val r1: ContainerOrConcept  = r.asInstanceOf[ContainerOrConcept]
    logger.Info("<<< 22")
    Array(r1)
  }
}
