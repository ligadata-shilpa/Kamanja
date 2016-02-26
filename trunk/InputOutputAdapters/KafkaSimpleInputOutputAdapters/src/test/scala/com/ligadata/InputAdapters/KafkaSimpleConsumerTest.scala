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

package com.ligadata.InputAdapters

import com.ligadata.InputOutputAdapterInfo._
import com.ligadata.KamanjaBase.DataDelimiters
import org.scalatest._

import util.control.Breaks._

import com.ligadata.testutils.docker.DockerManager
import com.ligadata.AdaptersConfiguration._

import scala.collection.JavaConversions._

private object MessageCounter {
  var messages: List[String] = List()
  var messagesIsPopulated = if(messages.size > 0) true else false
}

object SimpleStats extends CountersAdapter {
  override def addCntr(key: String, cnt: Long): Long = 0
  override def addCntr(cntrs: scala.collection.immutable.Map[String, Long]): Unit = {}
  override def getCntr(key: String): Long = 0
  override def getDispString(delim: String): String = ""
  override def copyMap: scala.collection.immutable.Map[String, Long] = null
}

object ExecContextObjImpl extends ExecContextObj {
  def CreateExecContext(input: InputAdapter, curPartitionKey: PartitionUniqueRecordKey, callerCtxt: InputAdapterCallerContext): ExecContext = {
    new ExecContextImpl(input, curPartitionKey, callerCtxt)
  }
}

class ExecContextImpl(val input: InputAdapter, val curPartitionKey: PartitionUniqueRecordKey, val callerCtxt: InputAdapterCallerContext) extends ExecContext {
  def execute(data: Array[Byte], format: String, uniqueKey: PartitionUniqueRecordKey, uniqueVal: PartitionUniqueRecordValue, readTmNanoSecs: Long, readTmMilliSecs: Long, ignoreOutput: Boolean, associatedMsg: String, delimiters: DataDelimiters): Unit = {
    MessageCounter.messages = MessageCounter.messages :+ new String(data)
  }
}


class KafkaSimpleConsumerTest extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll {

  private val dm = new DockerManager()
  private var containerId = ""

  var adapter: InputAdapter = _
  var inputMeta: Array[StartProcPartInfo] = _

  override def beforeAll: Unit = {
    val exposedPortMap = mapAsJavaMap(Map(2181 -> 2181, 9092 -> 9092)).asInstanceOf[java.util.Map[Integer, Integer]]
    val envVariables = mapAsJavaMap(Map("ADVERTISED_HOST" -> dm.getDockerHost(), "ADVERTISED_PORT" -> "9092"))
    val createTopicCmd = Array("bash", "/opt/kafka_2.11-0.8.2.1/bin/kafka-topics.sh", "--create",
      "--zookeeper", "localhost:2181", "--topic", "testin_1", "--replication-factor", "1", "--partitions", "8")

    containerId = dm.startContainer("spotify/kafka", exposedPortMap, envVariables)
    Thread sleep 5000
    dm.executeCmd(containerId, createTopicCmd: _*)

    try {
      val adapterConfiguration = new AdapterConfiguration
      adapterConfiguration.adapterSpecificCfg = s"""{ "HostList": "${dm.getDockerHost()}:9092", "TopicName" : "testin_1" }"""
      adapterConfiguration.className = "com.ligadata.InputAdapters.KafkaSimpleConsumer$"
      adapterConfiguration.dependencyJars = Set("jopt-simple-3.2.jar",
        "kafka_2.11-0.8.2.2.jar",
        "kafka-clients-0.8.2.2.jar",
        "metrics-core-2.2.0.jar",
        "zkclient-0.4.jar",
        "kamanjabase_2.11-1.0.jar",
        "kvbase_2.11-0.1.0.jar")
      adapterConfiguration.formatName = "CSV"
      adapterConfiguration.jarName = "kafkasimpleinputoutputadapters_2.11-1.0.jar"
      adapterConfiguration.Name = "TestIn_1"

      adapter = KafkaSimpleConsumer.CreateInputAdapter(adapterConfiguration, null, ExecContextObjImpl, SimpleStats)
      val adapterMeta: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = adapter.getAllPartitionEndValues

      inputMeta = adapterMeta.map(partMeta => {
        val info = new StartProcPartInfo
        info._key = partMeta._1
        info._val = partMeta._2
        info._validateInfoVal = partMeta._2
        info
      })

      if (inputMeta.length <= 0 || adapterMeta.length <= 0)
        throw new RuntimeException("Failed to retrieve input or adapter metadata for the topic 'testin_1'")

      //adapter.StartProcessing(inputMeta, false)
    }
    catch {
      case e: Exception => {
        println("FAILED TO MONITOR KAFKA TOPIC testin_1 WITH EXCEPTION:\n" + e)
        e.printStackTrace()
        //afterAll()
      }
    }
  }

  override def afterAll(): Unit = {
    adapter.StopProcessing
    dm.stopContainer(containerId)
  }

  before {
    MessageCounter.messages = List()
  }

  "Kafka Simple Consumer" should "connect to and read from a kafka topic" in {
    TestProducer.sendMessage("Hello World", s"${dm.getDockerHost()}:9092")
    adapter.StartProcessing(inputMeta, false)

    var counter = 0
    breakable {
      while (!MessageCounter.messagesIsPopulated) {
        if (counter >= 60)
          break
        Thread sleep 500
        counter += 1
      }
    }
    assert(MessageCounter.messages.contains("Hello World"))
  }


}