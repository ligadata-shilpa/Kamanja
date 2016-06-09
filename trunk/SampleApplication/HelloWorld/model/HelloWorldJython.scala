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

class HelloWorldJythonFactory(modelDef: ModelDef, nodeContext: NodeContext) extends ModelInstanceFactory(modelDef, nodeContext) {

  override def getModelName(): String = {
    "com.ligadata.kamanja.samples.models.HelloWorlJythondModel"
  }

  override def getVersion(): String = {
    "0.0.6" // ToDo: get from metadata
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
      |print 'jython>>> 2'
      |from com.ligadata.KamanjaBase import ContainerOrConcept
      |print 'jython>>> 3'
      |from com.ligadata.kamanja.samples.messages import msg1
      |print 'jython>>> 4'
      |from com.ligadata.kamanja.samples.messages import outmsg1
      |print 'jython>>> 5'
      |
      |class Model():
      |   def __init__(self):
      |       print 'jython>>> 10'
      |
      |   def execute(self, txnCtxt, execMsgsSet, matchedInputSetIndex, outputDefault):
      |       print 'jython>>> 20'
      |       inMsg = execMsgsSet[0]
      |       print 'jython>>> 21'
      |       if inMsg.id()!=111:
      |           print 'jython>>> 22'
      |           v = inMsg.in1()
      |           print "Early exit %d" % (v)
      |           return None
      |
      |       print 'jython>>> 23'
      |       output = outmsg1.createInstance();
      |       print 'jython>>> 24'
      |       output.set(0, inMsg.id())
      |       print 'jython>>> 25'
      |       output.set(1, inMsg.name())
      |       print 'jython>>> 26'
      |
      |       return output
      |
    """.stripMargin

  var props: Properties = new Properties()
  props.put("python.home", "/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system")
  props.put("python.console.encoding", "UTF-8")
  props.put("python.security.respectJavaAccessibility", "false")
  props.put("python.import.site", "false")

  var preprops: Properties = System.getProperties()

  PythonInterpreter.initialize(preprops, props, Array.empty[String]);

  // Create the jython object
  val interpreter = new PythonInterpreter()
  interpreter.compile(code)
  val modelgClass = interpreter.get("Model")
  val modelObject: PyObject = modelgClass.__call__()

  override def execute(txnCtxt: TransactionContext, execMsgsSet: Array[ContainerOrConcept], triggerdSetIndex: Int, outputDefault: Boolean): Array[ContainerOrConcept] = {
    val r: PyObject = modelObject.invoke("Execute", Array(Py.java2py(txnCtxt), Py.java2py(execMsgsSet), Py.java2py(0), Py.java2py(false)))
    val r1: ContainerOrConcept  = r.asInstanceOf[ContainerOrConcept]
    Array(r1)
  }
}
