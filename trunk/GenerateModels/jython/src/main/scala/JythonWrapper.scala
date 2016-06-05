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
package com.ligadata.kamaja.jython
import org.python.core.Py
import org.python.core.PyObject
import org.python.core.PyString
import org.python.util.PythonInterpreter
import com.ligadata.kamanja.metadata.ModelDef
import com.ligadata.KamanjaBase._

class JythonWrapperFactory(modelDef: ModelDef, nodeContext: NodeContext) extends ModelInstanceFactory(modelDef, nodeContext) {

  override def getModelName(): String = {
    "Jython" // ToDo: get from metadata
  }

  override def getVersion(): String = {
    "0.0.1" // ToDo: get from metadata
  }

  override def isValidMessage(msg: MessageContainerBase): Boolean = {
    true
  }
  override def createModelInstance(): ModelInstance = {
    return new JythonWrapperModel(this)
  }
}

class JythonWrapperModel(factory: ModelInstanceFactory) extends ModelInstance(factory) {

  // Create the jython object
  val interpreter = new PythonInterpreter()

  interpreter.compile("<string>")

  val modelgClass = interpreter.get("Model")
  val modelObject: PyObject = modelgClass.__call__()

  override def execute(txnCtxt: TransactionContext, execMsgsSet: Array[ContainerOrConcept], triggerdSetIndex: Int, outputDefault: Boolean): Array[ContainerOrConcept] = {
    val r: PyObject = modelObject.invoke("Execute", Py.java2py(txnCtxt), Py.java2py(execMsgsSet(0)))

    // Convert to output?
    val r1: ContainerOrConcept  = r.asInstanceOf[ContainerOrConcept]
    Array(r1)
  }

}
