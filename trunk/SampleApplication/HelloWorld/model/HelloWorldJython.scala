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

class HelloWorldJythonFactory(modelDef: ModelDef, nodeContext: NodeContext) extends ModelInstanceFactory(modelDef, nodeContext) {

  override def getModelName(): String = {
    "com.ligadata.kamanja.samples.models.HelloWorlJythondModel"
  }

  override def getVersion(): String = {
    "0.0.1" // ToDo: get from metadata
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
    |       if inMsg.in1()!=111:
    |           v = inMsg.in1()
    |           print "Early exit %d" % (v)
    |           return None
    |
    |       output = outmsg1.createInstance();
    |       output.set(0, inMsg.id())
    |       output.set(1, inMsg.name())
    |
    |       returnArr = [output];
    |       return returnArr;
    |
  """.stripMargin

  // Create the jython object
  val interpreter = new PythonInterpreter()
  interpreter.compile(code)
  val modelgClass = interpreter.get("Model")
  val modelObject: PyObject = modelgClass.__call__()

  override def execute(txnCtxt: TransactionContext, execMsgsSet: Array[ContainerOrConcept], triggerdSetIndex: Int, outputDefault: Boolean): Array[ContainerOrConcept] = {
    val r: PyObject = modelObject.invoke("Execute", Py.java2py(txnCtxt), Py.java2py(execMsgsSet), 0, false)
    val r1: ContainerOrConcept  = r.asInstanceOf[Array[ContainerOrConcept]]
    r1
  }
}
