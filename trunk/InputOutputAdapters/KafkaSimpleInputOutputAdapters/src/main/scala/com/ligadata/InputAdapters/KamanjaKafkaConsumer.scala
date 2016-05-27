package com.ligadata.InputAdapters

import java.util.Properties

import com.ligadata.AdaptersConfiguration.{KafkaQueueAdapterConfiguration, KafkaPartitionUniqueRecordValue, KafkaPartitionUniqueRecordKey}
import com.ligadata.Exceptions.KamanjaException
import com.ligadata.HeartBeat.MonitorComponentInfo
import com.ligadata.InputOutputAdapterInfo._
import com.ligadata.KamanjaBase.{NodeContext, DataDelimiters}
import kafka.api.{FetchResponse, FetchRequestBuilder}
import kafka.consumer.SimpleConsumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.PartitionInfo
import org.apache.logging.log4j.LogManager
import org.json4s.jackson.Serialization

import scala.actors.threadpool.Executors
import scala.util.control.Breaks._

/**
  *
  */
object KamanjaKafkaConsumer extends InputAdapterFactory {

  // Statistics Keys
  val ADAPTER_DESCRIPTION = "Kafka 0.10.0.0 Client"
  val PARTITION_COUNT_KEYS = "Partition Counts"
  val PARTITION_DEPTH_KEYS = "Partition Depths"
  val EXCEPTION_SUMMARY = "Exception Summary"

  def CreateInputAdapter(inputConfig: AdapterConfiguration, execCtxtObj: ExecContextFactory, nodeContext: NodeContext): InputAdapter = new KamanjaKafkaConsumer(inputConfig, execCtxtObj, nodeContext)
}


/**
  * Class that handles reading from Kafka Topics
  *
  * @param inputConfig
  * @param execCtxtObj
  * @param nodeContext
  */
class KamanjaKafkaConsumer(val inputConfig: AdapterConfiguration, val execCtxtObj: ExecContextFactory, val nodeContext: NodeContext) extends InputAdapter {

  private val qc = KafkaQueueAdapterConfiguration.GetAdapterConfig(inputConfig)
  private val LOG = LogManager.getLogger(getClass)

  LOG.debug("Creating a Kafka Adapter (client v0.9+) for topic:  " + qc.Name)
  private var metrics: collection.mutable.Map[String,Any] = collection.mutable.Map[String,Any]()
  private var isShutdown = false
  private val lock = new Object()
  private var partitonCounts: collection.mutable.Map[String,Long] = collection.mutable.Map[String,Long]()
  private var partitonDepths: collection.mutable.Map[String,Long] = collection.mutable.Map[String,Long]()
  private var partitionExceptions: collection.mutable.Map[String,ExceptionInfo] = collection.mutable.Map[String,ExceptionInfo]()
  private var msgCount: Long = 0


  private def populateConsumerProperties(): java.util.Properties = {
    var props = new Properties()
    props.put("bootstrap.servers", qc.hosts.mkString(","))
    props.put("enable.auto.commit", "false")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  /**
    * Start processing - will start a number of threads to read the Kafka queues for a topic.  The list of Hosts servicing a
    * given topic, and the topic have been set when this KafkaConsumer_V2 Adapter was instantiated.  The partitionIds should be
    * obtained via a prior call to the adapter.  One of the hosts will be a chosen as a leader to service the requests by the
    * spawned threads.
    *
    * @param ignoreFirstMsg Boolean - if true, ignore the first message sending to engine
    * @param partitionIds Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue, Long, PartitionUniqueRecordValue)] - an Array of partition ids
    */
  override def StartProcessing(partitionIds: Array[StartProcPartInfo], ignoreFirstMsg: Boolean): Unit = lock.synchronized {


  }

  /**
    * Called by the engine to determine all the partitions avaliable for this Topic.
    *
    * @return Array[PartitionUniqueRecordKey]
    */
  override def GetAllPartitionUniqueRecordKey: Array[PartitionUniqueRecordKey] = lock.synchronized {
    LOG.debug("Getting all partionas for " + qc.Name)
    var results: java.util.List[PartitionInfo] = null
    var partitionNames: scala.collection.mutable.ListBuffer[KafkaPartitionUniqueRecordKey] = scala.collection.mutable.ListBuffer()
    var cons: org.apache.kafka.clients.consumer.KafkaConsumer[String,String] = null

    try {
      // Create Consumer object
      val props: java.util.Properties = populateConsumerProperties()
      cons = new org.apache.kafka.clients.consumer.KafkaConsumer[String,String] (props)
      results = cons.partitionsFor(qc.topic)
    } catch {
      case e: Throwable => {
        LOG.error ("Exception processing PARTITIONSFOR request..  Retrying ",e)
        // gotta keep retrying
      }
    } finally {
      cons.close()
    }

    // return the info back to the Engine.  Just in case we end up with null result
    if (results == null) return {
      LOG.warn("Kafka broker returned a null during GetAllPartitionUniqueRecordKey for topic " + qc.topic)
      partitionNames.toArray
    }

    var iter = results.iterator
    while(iter.hasNext) {
      var thisRes = iter.next()
      var newVal = new KafkaPartitionUniqueRecordKey
      newVal.TopicName = thisRes.topic
      newVal.PartitionId = thisRes.partition
      partitionNames += newVal
      LOG.debug(" GetAllPartitions returned " +thisRes.partition + "  for topic " + thisRes.topic)
    }
    partitionNames.toArray
  }

  /**
    * Return an array of PartitionUniqueKey/PartitionUniqueRecordValues whre key is the partion and value is the offset
    * within the kafka queue where it begins.
    *
    * @return Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]
    */
  override def getAllPartitionBeginValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = lock.synchronized {
    return getKeyValues(kafka.api.OffsetRequest.EarliestTime)
  }

  /**
    * Return an array of PartitionUniqueKey/PartitionUniqueRecordValues whre key is the partion and value is the offset
    * within the kafka queue where it eds.
    *
    * @return Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]
    */
  override def getAllPartitionEndValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = lock.synchronized {
    return getKeyValues(kafka.api.OffsetRequest.LatestTime)
  }

  /**
    *
    */
  override def Shutdown(): Unit = lock.synchronized {
    StopProcessing
  }


  /**
    *
    * @param k
    * @return
    */
  override def DeserializeKey(k: String): PartitionUniqueRecordKey = {
    val key = new KafkaPartitionUniqueRecordKey
    try {
      LOG.debug("Deserializing Key:" + k)
      key.Deserialize(k)
    } catch {
      case e: Exception => {
        LOG.error("Failed to deserialize Key:%s.".format(k), e)
        throw e
      }
    }
    key
  }

  /**
    *
    * @param v
    * @return
    */
  override def DeserializeValue(v: String): PartitionUniqueRecordValue = {
    val vl = new KafkaPartitionUniqueRecordValue
    if (v != null) {
      try {
        LOG.debug("Deserializing Value:" + v)
        vl.Deserialize(v)
      } catch {
        case e: Exception => {
          LOG.error("Failed to deserialize Value:%s.".format(v), e)
          throw e
        }
      }
    }
    vl
  }





  private def getKeyValues(time: Long): Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = {
    var infoList = List[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]()
    return infoList.toArray
  }
  /**
    * Will stop all the running read threads only - a call to StartProcessing will restart the reading process
    */
  override def StopProcessing(): Unit = {
    isShutdown = true
    terminateReaderTasks
  }

  /**
    *
    * @return String - Simple Stats format TYPE/NAME/envCnt->COUNT
    */
  override def getComponentSimpleStats: String = {
    return "Input/"+qc.topic+"/evtCnt" + "->" + msgCount
  }

  /**
    *
    * @return MonitorComponentInfo
    */
  override def getComponentStatusAndMetrics: MonitorComponentInfo = {
    implicit val formats = org.json4s.DefaultFormats

    var depths:  Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = null

    try {
      depths = getAllPartitionEndValues
    } catch {
      case e: KamanjaException => {
        return new MonitorComponentInfo(AdapterConfiguration.TYPE_INPUT, qc.Name, KamanjaKafkaConsumer.ADAPTER_DESCRIPTION, "0", "0", Serialization.write(metrics).toString)
      }
      case e: Exception => {
        LOG.error ("KAFKA-ADAPTER: Unexpected exception determining kafka queue depths for " + qc.topic, e)
        return new MonitorComponentInfo(AdapterConfiguration.TYPE_INPUT, qc.Name, KamanjaKafkaConsumer.ADAPTER_DESCRIPTION, "0",  "0", Serialization.write(metrics).toString)
      }
    }

    partitonDepths.clear

    return new MonitorComponentInfo( AdapterConfiguration.TYPE_INPUT, qc.Name, KamanjaKafkaConsumer.ADAPTER_DESCRIPTION, "0",  "0",  Serialization.write(metrics).toString)
  }

  /**
    *  terminateReaderTasks - well, just what it says
    */
  private def terminateReaderTasks(): Unit = {

  }
}