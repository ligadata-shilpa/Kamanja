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

package com.ligadata.InputOutputAdapterInfo

import com.ligadata.KamanjaBase._
import com.ligadata.HeartBeat._

import scala.collection.mutable.ArrayBuffer

object AdapterConfiguration {
  val TYPE_INPUT = "Input_Adapter"
  val TYPE_OUTPUT = "Output_Adapter"
}

class AdapterConfiguration {
  // Name of the Adapter, KafkaQueue Name/MQ Name/File Adapter Logical Name/etc
  var Name: String = _
//  // CSV/JSON/XML for input adapter.
//  var formatName: String = _
//  // For output adapter it is just corresponding validate adapter name.
//  var validateAdapterName: String = _
//  // For input adapter it is just corresponding failed events adapter name.
//  var failedEventsAdapterName: String = _
//  // Queue Associated Message
//  var associatedMsg: String = _
  // Class where the Adapter can be loaded (Object derived from InputAdapterObj)
  var className: String = _
  // Jar where the className can be found
  var jarName: String = _
  // All dependency Jars for jarName
  var dependencyJars: Set[String] = _
  // adapter specific (mostly json) string
  var adapterSpecificCfg: String = _
//  // Delimiter String for keyAndValueDelimiter
//  var keyAndValueDelimiter: String = _
//  // Delimiter String for fieldDelimiter
//  var fieldDelimiter: String = _
//  var valueDelimiter: String = _ // Delimiter String for valueDelimiter
}

// Input Adapter Object to create Adapter
trait InputAdapterFactory {
  def CreateInputAdapter(inputConfig: AdapterConfiguration, execCtxtObj: ExecContextFactory, nodeContext: NodeContext): InputAdapter
}

class StartProcPartInfo {
  var _key: PartitionUniqueRecordKey = null
  var _val: PartitionUniqueRecordValue = null
  var _validateInfoVal: PartitionUniqueRecordValue = null
}

// Input Adapter
trait InputAdapter extends Monitorable {
  val nodeContext: NodeContext
  // NodeContext
  val inputConfig: AdapterConfiguration // Configuration

  def UniqueName: String = {
    // Making String from key
    return "{\"Name\" : \"%s\"}".format(inputConfig.Name)
  }

  def Category = "Input"

  def Shutdown: Unit

  def StopProcessing: Unit

  def StartProcessing(partitionInfo: Array[StartProcPartInfo], ignoreFirstMsg: Boolean): Unit

  // each value in partitionInfo is (PartitionUniqueRecordKey, PartitionUniqueRecordValue, Long, PartitionUniqueRecordValue). // key, processed value, Start transactionid, Ignore Output Till given Value (Which is written into Output Adapter) & processing Transformed messages (processing & total)
  def GetAllPartitionUniqueRecordKey: Array[PartitionUniqueRecordKey]

  def DeserializeKey(k: String): PartitionUniqueRecordKey

  def DeserializeValue(v: String): PartitionUniqueRecordValue

  def getAllPartitionBeginValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]

  def getAllPartitionEndValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]
}

// Output Adapter Object to create Adapter
trait OutputAdapterFactory {
  def CreateOutputAdapter(inputConfig: AdapterConfiguration, nodeContext: NodeContext): OutputAdapter
}

// Output Adapter
trait OutputAdapter extends AdaptersSerializeDeserializers with Monitorable {
  // NodeContext
  val nodeContext: NodeContext
  // Configuration
  val inputConfig: AdapterConfiguration

  def send(tnxCtxt: TransactionContext, outputContainers: Array[MessageContainerBase]): Unit = {
    val (outputContainers, serializedContainerData, serializerNames) = serialize(tnxCtxt, outputContainers)
    send(tnxCtxt, outputContainers, serializedContainerData, serializerNames)
  }

  // This is protected override method. After applying serialization, pass original messages, Serialized data & Serializer names
  protected def send(tnxCtxt: TransactionContext, outputContainers: Array[MessageContainerBase], serializedContainerData: Array[Array[Byte]], serializerNames: Array[String]): Unit

  def Shutdown: Unit

  def Category = "Output"
}

trait ExecContext extends AdaptersSerializeDeserializers {
  val input: InputAdapter
  val curPartitionKey: PartitionUniqueRecordKey
  val nodeContext: NodeContext

  def execute(data: Array[Byte], uniqueKey: PartitionUniqueRecordKey, uniqueVal: PartitionUniqueRecordValue, readTmNanoSecs: Long, readTmMilliSecs: Long): Unit = {
    val (msg, deserializerName) = deserialize(data)
    executeMessage(msg, data, uniqueKey, uniqueVal, readTmNanoSecs, readTmMilliSecs, deserializerName): Unit
  }

  protected def executeMessage(msg: MessageContainerBase, data: Array[Byte], uniqueKey: PartitionUniqueRecordKey, uniqueVal: PartitionUniqueRecordValue, readTmNanoSecs: Long, readTmMilliSecs: Long, deserializerName: String): Unit
}

trait ExecContextFactory {
  def CreateExecContext(input: InputAdapter, curPartitionKey: PartitionUniqueRecordKey, nodeContext: NodeContext): ExecContext
}

trait PartitionUniqueRecordKey {
  val Type: String

  // Type of the Key -- For now putting File/Kafka like that. This is mostly for readable purpose (for which adapter etc)
  def Serialize: String

  // Making String from key
  def Deserialize(key: String): Unit // Making Key from Serialized String
}

trait PartitionUniqueRecordValue {
  def Serialize: String

  // Making String from Value
  def Deserialize(key: String): Unit // Making Value from Serialized String
}


