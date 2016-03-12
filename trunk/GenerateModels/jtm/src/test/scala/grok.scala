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
package com.ligadata.jtm.test.grok

class msg1 {
  var in1 : Int = 0
  var in2 : Int = 0
  var in3 : String = ""
  var in4 : String = ""

  var rowNumber : Int = 0
  var transactionId : Int = 0
  var timePartitionData : Int = 0
}

import com.ligadata.KamanjaBase._
import com.ligadata.KvBase.TimeRange
import com.ligadata.kamanja.metadata.ModelDef
import com.ligadata.Utils._
import org.aicer.grok.dictionary.GrokDictionary

import org.apache.commons.io.FileUtils
import java.io.File
import java.io.StringReader

class Factory(modelDef: ModelDef, nodeContext: NodeContext) extends ModelInstanceFactory(modelDef, nodeContext) {
  override def isValidMessage(msg: MessageContainerBase): Boolean = {
    msg.isInstanceOf[msg1]
  }
  override def createModelInstance(): ModelInstance = return new Model(this)
  override def getModelName: String = "com.ligadata.jtm.test.grok"
  override def getVersion: String = "0.0.1"
  override def createResultObject(): ModelResultBase = new MappedModelResults()
}
class Model(factory: ModelInstanceFactory) extends ModelInstance(factory) {

  // Produce the grok instance
  lazy val name_grok_instance: GrokDictionary = {
    val i = new GrokDictionary
    // If builtInDictionary
    i.addBuiltInDictionaries()
    // Files for load
    Seq("filename").foreach(fname => {
      val f = getClass.getResource(fname).getPath
      i.addDictionary(new File(f))
    })
    // Patterns to load
    Map("DOMAINTLD" -> "[a-zA-Z]+", "EMAIL" -> "%{NOTSPACE}@%{WORD}\\.%{DOMAINTLD}").foreach(f =>
      i.addDictionary(new StringReader(f._1 + " " + f._2)
      ))
    // Finalize
    i.bind()
    i
  }

  // Compile all unique expressions
  lazy val p1 = name_grok_instance.compileExpression("{EMAIL: email}")
  lazy val p2 = name_grok_instance.compileExpression("{URLDOMAIN: domain}")

  override def execute(txnCtxt: TransactionContext, outputDefault: Boolean): ModelResultBase = {
    //
    def exeGenerated_test1_1(msg1: msg1): Array[Result] = {
      // in scala, type could be optional
      val out3: Int = msg1.in1 + 1000
      def process_o1(): Array[Result] = {
        lazy val p1_r = p1.extractNamedGroups(msg1.in3.toString)
        if (!(msg1.in2 != -1 && msg1.in2 < 100)) return Array.empty[Result]
        val t1: String = "s:" + (msg1.in2).toString

        Array[Result](new Result("rowNumber", msg1.rowNumber),
          new Result("transactionId", msg1.transactionId),
          new Result("out1", msg1.in1),
          new Result("out4", if(p1_r.containsKey("email")) p1_r.get("email") else ""),
          new Result("out2", t1),
          new Result("timePartitionData", msg1.timePartitionData),
          new Result("out3", out3))
      }
      process_o1()
    }
    //
    // ToDo: we expect an array of messages
    //
    val msgs = Array(txnCtxt.getMessage()).map(m => m.FullName -> m).toMap
    // Evaluate messages
    //
    val msg1 = msgs.get("msg1").getOrElse(null).asInstanceOf[msg1]
    // Main dependency -> execution check
    //
    val results: Array[Result] =
      if(msg1!=null) {
        exeGenerated_test1_1(msg1)
      } else {
        Array.empty[Result]
      }
    factory.createResultObject().asInstanceOf[MappedModelResults].withResults(results)
  }
}
