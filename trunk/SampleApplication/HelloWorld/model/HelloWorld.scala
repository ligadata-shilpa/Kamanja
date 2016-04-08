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

package com.ligadata.samples.models

import com.ligadata.KamanjaBase._
import com.ligadata.kamanja.metadata.ModelDef;

class HelloWorldModelFactory(modelDef: ModelDef, nodeContext: NodeContext) extends ModelInstanceFactory(modelDef, nodeContext) {
  override def isValidMessage(msg: ContainerInterface): Boolean = return false // msg.isInstanceOf[msg1]
  override def createModelInstance(): ModelInstance = return new HelloWorldModel(this)
  override def getModelName: String = "HelloWorldModel" 
  override def getVersion: String = "0.0.1"
  override def createResultObject(): ModelResultBase = new MappedModelResults()
}

class HelloWorldModel(factory: ModelInstanceFactory) extends ModelInstance(factory) {
   override def execute(txnCtxt: TransactionContext, execMsgsSet: Array[ContainerOrConcept], triggerdSetIndex: Int, outputDefault: Boolean): Array[ContainerOrConcept] = {
     var helloWorld : msg1 =  execMsgsSet(0).asInstanceOf[msg1] // This run should trigger when we have only msg1
        if(helloWorld.score!=1)
          return null;
        return Array(helloWorld);
   }
}
