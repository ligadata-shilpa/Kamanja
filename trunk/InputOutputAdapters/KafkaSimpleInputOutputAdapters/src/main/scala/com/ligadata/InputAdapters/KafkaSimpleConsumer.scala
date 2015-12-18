
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

package com.ligadata.InputAdapters

import com.ligadata.AdaptersConfiguration.{ KafkaPartitionUniqueRecordKey, KafkaPartitionUniqueRecordValue, KafkaQueueAdapterConfiguration }
import com.ligadata.InputOutputAdapterInfo._
import kafka.api._
import kafka.common.TopicAndPartition
import scala.actors.threadpool.{ TimeUnit, ExecutorService, Executors }
import scala.util.control.Breaks._
import kafka.consumer.{ SimpleConsumer }
import java.net.{ InetAddress }
import org.apache.logging.log4j.{ Logger, LogManager }
import scala.collection.mutable.Map
import com.ligadata.Exceptions.{FatalAdapterException, StackTrace}
import com.ligadata.KamanjaBase.DataDelimiters
import scala.collection.breakOut

object KafkaSimpleConsumer extends InputAdapterObj {
  val METADATA_REQUEST_CORR_ID = 2
  val QUEUE_FETCH_REQUEST_TYPE = 1
  val METADATA_REQUEST_TYPE = "metadataLookup"
  val MAX_FAILURES = 2
  val MONITOR_FREQUENCY = 10000 // Monitor Topic queues every 20 seconds
  val SLEEP_DURATION = 1000 // Allow 1 sec between unsucessful fetched
  var CURRENT_BROKER: String = _
  val FETCHSIZE = 64 * 1024
  val ZOOKEEPER_CONNECTION_TIMEOUT_MS = 3000
  val MAX_TIMEOUT = 60000
  val INIT_TIMEOUT = 250

  def CreateInputAdapter(inputConfig: AdapterConfiguration, callerCtxt: InputAdapterCallerContext, execCtxtObj: ExecContextObj, cntrAdapter: CountersAdapter): InputAdapter = new KafkaSimpleConsumer(inputConfig, callerCtxt, execCtxtObj, cntrAdapter)
}

class KafkaSimpleConsumer(val inputConfig: AdapterConfiguration, val callerCtxt: InputAdapterCallerContext, val execCtxtObj: ExecContextObj, cntrAdapter: CountersAdapter) extends InputAdapter {
  val input = this
  private val lock = new Object()
  private val LOG = LogManager.getLogger(getClass)
  private var isQuiesced = false
  private var startTime: Long = 0

  private val qc = KafkaQueueAdapterConfiguration.GetAdapterConfig(inputConfig)

  LOG.debug("KAFKA ADAPTER: allocating kafka adapter for " + qc.hosts.size + " broker hosts")

  private var numberOfErrors: Int = _
  private var replicaBrokers: Set[String] = Set()
  private var readExecutor: ExecutorService = _
  private val kvs = scala.collection.mutable.Map[Int, (KafkaPartitionUniqueRecordKey, KafkaPartitionUniqueRecordValue, KafkaPartitionUniqueRecordValue)]()
  private val kvs_per_threads = scala.collection.mutable.Map[Int, scala.collection.mutable.Map[Int, (KafkaPartitionUniqueRecordKey, KafkaPartitionUniqueRecordValue, KafkaPartitionUniqueRecordValue)]]()
  private var clientName: String = _

  // Heartbeat monitor related variables.
  private var hbRunning: Boolean = false
  private var hbTopicPartitionNumber = -1
  private val hbExecutor = Executors.newFixedThreadPool(qc.hosts.size)

  private var timeoutTimer = KafkaSimpleConsumer.INIT_TIMEOUT
  // See how many cores there are on the system.
  var numOfCores = Runtime.getRuntime.availableProcessors

  /**
   *  This will stop all the running reading threads and close all the underlying connections to Kafka.
   */
  override def Shutdown(): Unit = lock.synchronized {
    StopProcessing
  }

  /**
   * Will stop all the running read threads only - a call to StartProcessing will restart the reading process
   */
  def StopProcessing(): Unit = {
    terminateReaderTasks
    terminateHBTasks
  }

  private def getTimeoutTimer: Long = {
    timeoutTimer = timeoutTimer * 2
    if (timeoutTimer > KafkaSimpleConsumer.MAX_TIMEOUT) {
      timeoutTimer = KafkaSimpleConsumer.MAX_TIMEOUT
      return timeoutTimer
    }
    return timeoutTimer
  }

  private def resetTimeoutTimer: Unit = {
    timeoutTimer = KafkaSimpleConsumer.INIT_TIMEOUT
  }

  /**
   * Start processing - will start a number of threads to read the Kafka queues for a topic.  The list of Hosts servicing a
   * given topic, and the topic have been set when this KafkaConsumer_V2 Adapter was instantiated.  The partitionIds should be
   * obtained via a prior call to the adapter.  One of the hosts will be a chosen as a leader to service the requests by the
   * spawned threads.
   * @param maxParts Int - Number of Partitions
   * @param partitionIds Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue, Long, PartitionUniqueRecordValue)] - an Array of partition ids
   */
  def StartProcessing(partitionIds: Array[StartProcPartInfo], ignoreFirstMsg: Boolean): Unit = lock.synchronized {

    LOG.info("START_PROCESSING CALLED")
    // Check to see if this already started
    if (startTime > 0) {
      LOG.error("KAFKA-ADAPTER: already started, or in the process of shutting down")
    }
    startTime = System.nanoTime
    LOG.debug("KAFKA-ADAPTER: Starting to read Kafka queues for topic: " + qc.topic)

    if (partitionIds == null || partitionIds.size == 0) {
      LOG.error("KAFKA-ADAPTER: Cannot process the kafka queue request, invalid parameters - number")
      return
    }

    // Get the data about the request and set the instancePartition list.
    val partitionInfo = partitionIds.map(quad => {
      (quad._key.asInstanceOf[KafkaPartitionUniqueRecordKey],
        quad._val.asInstanceOf[KafkaPartitionUniqueRecordValue],
        quad._validateInfoVal.asInstanceOf[KafkaPartitionUniqueRecordValue])
    })

    qc.instancePartitions = partitionInfo.map(partQuad => { partQuad._1.PartitionId }).toSet

    // Make sure the data passed was valid.
    if (qc.instancePartitions == null || qc.instancePartitions.isEmpty) {
      LOG.error("KAFKA-ADAPTER: Cannot process the kafka queue request, invalid parameters - partition instance list")
      return
    }

    // Figure out the size of the thread pool to use and create that pool
    val threads = numOfCores

    readExecutor = Executors.newFixedThreadPool(threads)

    // Create a Map of all the partiotion Ids.
    kvs.clear
    kvs_per_threads.clear
    partitionInfo.foreach(quad => {
      kvs(quad._1.PartitionId) = quad
    })

    // Partition the incoming sack of Partitions into buckets.  For now assume the number of threads executing these will
    // be the number of logical processors available to the JVM
    for (indx <- 1 to threads) {
     // kvs_per_threads(indx) = scala.collection.mutable.Map[Int, (KafkaPartitionUniqueRecordKey, KafkaPartitionUniqueRecordValue, KafkaPartitionUniqueRecordValue)]()
      var partitionBucket = scala.collection.mutable.Map[Int, (KafkaPartitionUniqueRecordKey, KafkaPartitionUniqueRecordValue, KafkaPartitionUniqueRecordValue)]()
      println("Creating Bucket for thread " + indx)

      // Find all Partitions that will be processed by this thread. use the simple mod operation.
      kvs.foreach(partition => {
        if (((partition._1 % threads) + 1) == indx) {
          partitionBucket(partition._1) = kvs(partition._1)
        }
      })

      // Add all the partitions that apply to this thread, into the map for that thread
      kvs_per_threads(indx) = partitionBucket
    }

    val delimiters = new DataDelimiters()
    delimiters.keyAndValueDelimiter = qc.keyAndValueDelimiter
    delimiters.fieldDelimiter = qc.fieldDelimiter
    delimiters.valueDelimiter = qc.valueDelimiter

    // Enable the adapter to process
    isQuiesced = false
    LOG.debug("KAFKA-ADAPTER: Starting " + kvs.size + " threads to process partitions")

    // Schedule a task to perform a read from a give partition.
    //kvs.foreach(kvsElement => {
    kvs_per_threads.foreach(kvsThreadElements =>{
      readExecutor.execute(new Runnable() {
        override def run() {
         // val partitionId = kvsElement._1
         // val partition = kvsElement._2

          // Get the map of  partitions to be processed by this thread
          val partitionsForThread = kvsThreadElements._2
          val leadBrokers: scala.collection.mutable.Map[Int, String] = scala.collection.mutable.Map[Int, String]()
          val readOffsets: scala.collection.mutable.Map[Int,Long] = scala.collection.mutable.Map[Int,Long]()
          val uniqueRecordValues: scala.collection.mutable.Map[Int,Long] = scala.collection.mutable.Map[Int,Long]()
          val messagesProcessedPerPartition: scala.collection.mutable.Map[Int,Int] = scala.collection.mutable.Map[Int,Int]()
          val uniqueKeys: scala.collection.mutable.Map[Int,KafkaPartitionUniqueRecordKey] = scala.collection.mutable.Map[Int,KafkaPartitionUniqueRecordKey]()
          val uniqueValues: scala.collection.mutable.Map[Int,KafkaPartitionUniqueRecordValue] = scala.collection.mutable.Map[Int,KafkaPartitionUniqueRecordValue]()
          val execThread: ExecContext = null
          val clientNames:  scala.collection.mutable.Map[Int,String] = scala.collection.mutable.Map[Int,String]()
          val consumers: scala.collection.mutable.Map[Int,SimpleConsumer] =  scala.collection.mutable.Map[Int,SimpleConsumer]()

          // Intitialize somve values, find out all the Lead brokers
          partitionsForThread.foreach(partition => {
            val pID = partition._1
            val pInfo = partition._2

            readOffsets(pID) = -1
            messagesProcessedPerPartition(pID) = 0
            uniqueRecordValues(pID) =  if (ignoreFirstMsg) pInfo._3.Offset else pInfo._3.Offset - 1

            clientNames(pID) = "Client" + qc.Name + "/" + pID
            var uKey = new KafkaPartitionUniqueRecordKey
            uKey.Name = qc.Name
            uKey.TopicName = qc.topic
            uKey.PartitionId = pID
            uniqueKeys(pID) = uKey
            uniqueValues(pID) = new KafkaPartitionUniqueRecordValue

            leadBrokers(partition._1) = getKafkaConfigId(findLeader(qc.hosts, partition._1))
            var thisOffset = getKeyValueForPartition(leadBrokers(pID), pID, kafka.api.OffsetRequest.EarliestTime)
            if (pInfo._2.Offset > thisOffset) {
              thisOffset = pInfo._2.Offset
            }
            readOffsets(pID) = thisOffset

            if (thisOffset == -1) {
              LOG.error("KAFKA-ADAPTER: Unable to initialize new reader thread for partition {" + pID + "} starting at offset " + thisOffset + " on server - " + leadBrokers(pID) + ", Invalid OFFSET")
              return
            }

            LOG.info("KAFKA-ADAPTER: Initializing new reader thread for partition {" + pID + "} starting at offset " + thisOffset + " on server - " + leadBrokers(pID))

            val brokerId = convertIp(leadBrokers(pID))
            val brokerName = brokerId.split(":")
            consumers(pID) = null

            while (consumers(pID) == null && !isQuiesced) {
              try {
                consumers(pID) = new SimpleConsumer(brokerName(0), brokerName(1).toInt,
                                                    KafkaSimpleConsumer.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
                                                    KafkaSimpleConsumer.FETCHSIZE,
                                                    KafkaSimpleConsumer.METADATA_REQUEST_TYPE)
              } catch {
                case e: Exception => {
                  LOG.error("KAFKA ADAPTER: Failure connecting to Kafka server, retrying", e)
                  Thread.sleep(getTimeoutTimer)
                }
              }
            }
            resetTimeoutTimer
          })

          // OK, the connection to the server is ready... start processing until you fail enough times.
          while (!isQuiesced) {

            // for each partition in this thread
            partitionsForThread.foreach(partition => {
              val pID = partition._1
              val pInfo = partition._2

              val fetchReq = new FetchRequestBuilder().clientId(clientNames(pID)).addFetch(qc.topic, pID, readOffsets(pID), KafkaSimpleConsumer.FETCHSIZE).build()
              var fetchResp: FetchResponse = null
              var isFetchError = false

              // Call the broker and get a response.
              while ((fetchResp == null || isFetchError) && !isQuiesced) {
                try {
                  fetchResp = consumers(pID).fetch(fetchReq)
                  isFetchError = false
                  if (fetchResp.hasError) {
                    isFetchError = true
                    LOG.warn("KAFKA ADAPTER: Error fetching topic " + qc.topic + ", partition " + pID + ", retrying due to an error " + fetchResp.errorCode(qc.topic, pID))
                    Thread.sleep(getTimeoutTimer)
                  } else {
                    resetTimeoutTimer
                  }
                } catch {
                  case e: Exception => {
                    LOG.error("KAFKA ADAPTER: Failure fetching topic "+qc.topic+", partition " + pID + ", retrying", e)
                    Thread.sleep(getTimeoutTimer)
                  }
                }
              }

              // Got ourselves a response...
              // If we are here under shutdown conditions.. cleanup and bail
              if (isQuiesced) {
                if (consumers(pID) != null) { consumers(pID).close }
                return
              }


            }


            val ignoreTillOffset = if (ignoreFirstMsg) partition._2.Offset else partition._2.Offset - 1
            // Successfuly read from the Kafka Adapter - Process messages
            fetchResp.messageSet(qc.topic, partitionId).foreach(msgBuffer => {
              val bufferPayload = msgBuffer.message.payload
              val message: Array[Byte] = new Array[Byte](bufferPayload.limit)
              readOffset = msgBuffer.nextOffset
              breakable {
                val readTmNs = System.nanoTime
                val readTmMs = System.currentTimeMillis
                messagesProcessed = messagesProcessed + 1

                // Engine in interested in message at OFFSET + 1, Because I cannot guarantee that offset for a partition
                // is increasing by one, and I cannot simple set the offset to offset++ since that can cause our of
                // range errors on the read, we simple ignore the message by with the offset specified by the engine.
                if (msgBuffer.offset <= ignoreTillOffset) {
                  LOG.debug("KAFKA-ADAPTER: skipping a message at  Broker: " + leadBroker + "_" + partitionId + " OFFSET " + msgBuffer.offset + " " + new String(message, "UTF-8") + " - previously processed! ")
                  break
                }

                // OK, present this message to the Engine.
                bufferPayload.get(message)
                LOG.debug("KAFKA-ADAPTER: Broker: " + leadBroker + "_" + partitionId + " OFFSET " + msgBuffer.offset + " Message: " + new String(message, "UTF-8"))

                // Create a new EngineMessage and call the engine.
                if (execThread == null) {
                  execThread = execCtxtObj.CreateExecContext(input, uniqueKey, callerCtxt)
                }

                uniqueVal.Offset = msgBuffer.offset
                val dontSendOutputToOutputAdap = uniqueVal.Offset <= uniqueRecordValue
                execThread.execute(message, qc.formatOrInputAdapterName, uniqueKey, uniqueVal, readTmNs, readTmMs, dontSendOutputToOutputAdap, qc.associatedMsg, delimiters)

                val key = Category + "/" + qc.Name + "/evtCnt"
                cntrAdapter.addCntr(key, 1) // for now adding each row
              }

            })

            try {
              // Sleep here, only if input parm for sleep is set and we haven't gotten any messages on the previous kafka call.
              if ((qc.noDataSleepTimeInMs > 0) && (messagesProcessed == 0)) {
                Thread.sleep(qc.noDataSleepTimeInMs)
              }
              messagesProcessed = 0

            } catch {
              case e: java.lang.InterruptedException =>
                {
                  val stackTrace = StackTrace.ThrowableTraceString(e)
                  LOG.debug("KAFKA ADAPTER: Forcing down the Consumer Reader thread" + "\nStackTrace:" + stackTrace)
                }
            }
          }
          if (consumer != null) { consumer.close }
        }
      })
    })

  }

  /**
   * getServerInfo - returns information about hosts and their coresponding partitions.
   * @return Array[PartitionUniqueRecordKey] - return data
   */
  def GetAllPartitionUniqueRecordKey: Array[PartitionUniqueRecordKey] = lock.synchronized {
    // iterate through all the simple consumers - collect the metadata about this topic on each specified host
    var partitionRecord: scala.collection.mutable.Set[String] = scala.collection.mutable.Set[String]()

    val topics: Array[String] = Array(qc.topic)
    val metaDataReq = new TopicMetadataRequest(topics, KafkaSimpleConsumer.METADATA_REQUEST_CORR_ID)
    var partitionNames: List[KafkaPartitionUniqueRecordKey] = List()

    LOG.debug("KAFKA-ADAPTER - Querying kafka for Topic " + qc.topic + " metadata(partitions)")

    qc.hosts.foreach(broker => {
      val brokerName = broker.split(":")
      var partConsumer: SimpleConsumer = null
      try {
        partConsumer = new SimpleConsumer(brokerName(0), brokerName(1).toInt,
                                          KafkaSimpleConsumer.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
                                          KafkaSimpleConsumer.FETCHSIZE,
                                          KafkaSimpleConsumer.METADATA_REQUEST_TYPE)
      } catch {
        case e: Exception => throw FatalAdapterException("Unable to create connection to Kafka Server ", e)
      }

      try {

        // Keep retrying to connect until MAX number of tries is reached.
        var retryCount = 0
        var result = doSend(partConsumer,metaDataReq)
        while(result._1 == null && !isQuiesced) {
          if (retryCount > KafkaSimpleConsumer.MAX_FAILURES)
            throw FatalAdapterException("Failed to retrieve Topic Metadata for defined partitions", result._2)
          retryCount += 1
          Thread.sleep(1000)
          result = doSend(partConsumer,metaDataReq)
        }

        val metaData = result._1.topicsMetadata
        metaData.foreach(topicMeta => {
          topicMeta.partitionsMetadata.foreach(partitionMeta => {
            val uniqueKey = new KafkaPartitionUniqueRecordKey
            uniqueKey.PartitionId = partitionMeta.partitionId
            uniqueKey.Name = qc.Name
            uniqueKey.TopicName = qc.topic
            if (!partitionRecord.contains(qc.topic + partitionMeta.partitionId.toString)) {
              partitionNames = uniqueKey :: partitionNames
              partitionRecord = partitionRecord + (qc.topic + partitionMeta.partitionId.toString)
            }
          })
        })
      } catch {
        case fae: FatalAdapterException => throw fae
        case npe: NullPointerException => {
          if (isQuiesced) LOG.warn("Kafka Simple Consumer is shutting down during kafka call for partition information - ignoring the call")
        }
        case e: Exception => throw FatalAdapterException("failed to SEND MetadataRequest to Kafka Server ", e)
      } finally {
        if (partConsumer != null) { partConsumer.close }
      }
    })
    return partitionNames.toArray
  }

  /**
   *
   * @param consumer
   * @param req
   * @return
   */
  private def doSend(consumer: SimpleConsumer, req: TopicMetadataRequest): (kafka.api.TopicMetadataResponse,Exception) = {
    try {
      val metaDataResp: kafka.api.TopicMetadataResponse = consumer.send(req)
      return (metaDataResp,null)
    } catch {
      case e: Exception => return (null,e)
    }
  }


  /**
   * Return an array of PartitionUniqueKey/PartitionUniqueRecordValues whre key is the partion and value is the offset
   * within the kafka queue where it begins.
   * @return Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]
   */
  override def getAllPartitionBeginValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = lock.synchronized {
    return getKeyValues(kafka.api.OffsetRequest.EarliestTime)
  }

  /**
   * Return an array of PartitionUniqueKey/PartitionUniqueRecordValues whre key is the partion and value is the offset
   * within the kafka queue where it eds.
   * @return Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]
   */
  override def getAllPartitionEndValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = lock.synchronized {
    return getKeyValues(kafka.api.OffsetRequest.LatestTime)
  }

  override def DeserializeKey(k: String): PartitionUniqueRecordKey = {
    val key = new KafkaPartitionUniqueRecordKey
    try {
      LOG.debug("Deserializing Key:" + k)
      key.Deserialize(k)
    } catch {
      case e: Exception => {
        LOG.error("Failed to deserialize Key:%s. Reason:%s Message:%s".format(k, e.getCause, e.getMessage))
        throw e
      }
    }
    key
  }

  override def DeserializeValue(v: String): PartitionUniqueRecordValue = {
    val vl = new KafkaPartitionUniqueRecordValue
    if (v != null) {
      try {
        LOG.debug("Deserializing Value:" + v)
        vl.Deserialize(v)
      } catch {
        case e: Exception => {
          LOG.error("Failed to deserialize Value:%s. Reason:%s Message:%s".format(v, e.getCause, e.getMessage))
          throw e
        }
      }
    }
    vl
  }

  /**
   *  Find a leader of for this topic for a given partition.
   */
  private def findLeader(brokers: Array[String], inPartition: Int): kafka.api.PartitionMetadata = lock.synchronized {
    var leaderMetadata: kafka.api.PartitionMetadata = null

    LOG.debug("KAFKA-ADAPTER: Looking for Kafka Topic Leader for partition " + inPartition)
    try {
      breakable {
        brokers.foreach(broker => {
          // Create a connection to this broker to obtain the metadata for this broker.
          val brokerName = broker.split(":")
          var llConsumer: SimpleConsumer = null
          try {
            llConsumer = new SimpleConsumer(brokerName(0), brokerName(1).toInt,
                                            KafkaSimpleConsumer.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
                                            KafkaSimpleConsumer.FETCHSIZE,
                                            KafkaSimpleConsumer.METADATA_REQUEST_TYPE)
          } catch {
            case e: Exception => throw FatalAdapterException("Unable to create connection to Kafka Server ", e)
          }
          val topics: Array[String] = Array(qc.topic)
          val llReq = new TopicMetadataRequest(topics, KafkaSimpleConsumer.METADATA_REQUEST_CORR_ID)

          // get the metadata on the llConsumer
          try {
         //   val llResp: kafka.api.TopicMetadataResponse = llConsumer.send(llReq)

            // Keep retrying to connect until MAX number of tries is reached.
            var retryCount = 0
            var result = doSend(llConsumer,llReq)
            while(result._1 == null && !isQuiesced) {
              if (retryCount > KafkaSimpleConsumer.MAX_FAILURES)
                throw FatalAdapterException("Failed to retrieve Topic Metadata while searching for LEADER node", result._2)
              retryCount += 1
              Thread.sleep(1000)
              result = doSend(llConsumer,llReq)
            }

            val metaData = result._1.topicsMetadata

            // look at each piece of metadata, and analyze its partitions
            metaData.foreach(metaDatum => {
              val partitionMetadata = metaDatum.partitionsMetadata
              partitionMetadata.foreach(partitionMetadatum => {
                // If we found the partitionmetadatum for the desired partition then this must be the leader.
                if (partitionMetadatum.partitionId == inPartition) {
                  // Create a list of replicas to be used in case of a fetch failure here.
                  replicaBrokers.empty
                  partitionMetadatum.replicas.foreach(replica => {
                    replicaBrokers = replicaBrokers + (replica.host)
                  })
                  leaderMetadata = partitionMetadatum
                }
              })
            })
          } catch {
            case fae: FatalAdapterException => throw fae
            case npe: NullPointerException => {
              if (isQuiesced) LOG.warn("Kafka Simple Consumer is shutting down during kafka call looking for leader - ignoring the call")
            }
            case e: Exception => throw FatalAdapterException("failed to SEND MetadataRequest to Kafka Server ", e)
          } finally {
            if (llConsumer != null) llConsumer.close()
          }
        })
      }

    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        LOG.debug("KAFKA ADAPTER - Fatal Error for FindLeader for partition " + inPartition + "\nStackTrace:" + stackTrace)
      }
    }
    return leaderMetadata;
  }

  /*
* getKeyValues - get the values from the OffsetMetadata call and combine them into an array
 */
  private def getKeyValues(time: Long): Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = {
    var infoList = List[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]()
    // Always get the complete list of partitions for this.
    val kafkaKnownParitions = GetAllPartitionUniqueRecordKey
    val partitionList = kafkaKnownParitions.map(uKey => { uKey.asInstanceOf[KafkaPartitionUniqueRecordKey].PartitionId }).toSet

    // Now that we know for sure we have a partition list.. process them
    partitionList.foreach(partitionId => {
      val offset = getKeyValueForPartition(getKafkaConfigId(findLeader(qc.hosts, partitionId)), partitionId, time)
      val rKey = new KafkaPartitionUniqueRecordKey
      val rValue = new KafkaPartitionUniqueRecordValue
      rKey.PartitionId = partitionId
      rKey.Name = qc.Name
      rKey.TopicName = qc.topic
      rValue.Offset = offset
      infoList = (rKey, rValue) :: infoList
    })
    return infoList.toArray
  }

  /**
   * getKeyValueForPartition - get the valid offset range in a given partition.
   */
  private def getKeyValueForPartition(leadBroker: String, partitionId: Int, timeFrame: Long): Long = {
    var offset: Long = -1
    var llConsumer: kafka.javaapi.consumer.SimpleConsumer = null
    val brokerName = leadBroker.split(":")

    try{
      llConsumer = new kafka.javaapi.consumer.SimpleConsumer(brokerName(0), brokerName(1).toInt,
                                                               KafkaSimpleConsumer.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
                                                               KafkaSimpleConsumer.FETCHSIZE,
                                                               KafkaSimpleConsumer.METADATA_REQUEST_TYPE)
    } catch {
      case e: Exception => throw FatalAdapterException("Unable to create connection to Kafka Server ", e)
    }

    try {
      // Set up the request object
      val jtap: kafka.common.TopicAndPartition = kafka.common.TopicAndPartition(qc.topic.toString, partitionId)
      val requestInfo: java.util.HashMap[TopicAndPartition, PartitionOffsetRequestInfo] = new java.util.HashMap[TopicAndPartition, PartitionOffsetRequestInfo]()
      requestInfo.put(jtap, new PartitionOffsetRequestInfo(timeFrame, 1))
      val offsetRequest: kafka.javaapi.OffsetRequest = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion, clientName)

      // Issue the call
      // val response: kafka.javaapi.OffsetResponse = llConsumer.getOffsetsBefore(offsetRequest)
      var retryCount = 0
      var result = doSendJavaConsumer(llConsumer,offsetRequest)
      while(result._1 == null && !isQuiesced) {
        if (retryCount > KafkaSimpleConsumer.MAX_FAILURES)
          throw FatalAdapterException("Failed to retrieve Topic Metadata while searching for LEADER node", result._2)
        retryCount += 1
        Thread.sleep(1000)
        result = doSendJavaConsumer(llConsumer,offsetRequest)
      }

      // Return the value, or handle the error
      if (result._1.hasError) {
        LOG.error("KAFKA ADAPTER: error occured trying to find out the valid range for partition {" + partitionId + "}")
      } else {
        val offsets: Array[Long] = result._1.offsets(qc.topic.toString, partitionId)
        offset = offsets(0)
      }
    } catch {
      case fae: FatalAdapterException => throw fae
      case npe: NullPointerException => {
        if (isQuiesced) LOG.warn("Kafka Simple Consumer is shutting down during kafka call looking for offsets - ignoring the call")
      }
      case e: java.lang.Exception => {
        LOG.error("KAFKA ADAPTER: Exception during offset inquiry request for partiotion {" + partitionId + "}")
      }
    } finally {
      if (llConsumer != null) { llConsumer.close }
    }
    return offset
  }


  /**
   *
   * @param consumer
   * @param req
   * @return
   */
  private def doSendJavaConsumer(consumer: kafka.javaapi.consumer.SimpleConsumer, req: kafka.javaapi.OffsetRequest): (kafka.javaapi.OffsetResponse,Exception) = {
    try {
      val metaDataResp: kafka.javaapi.OffsetResponse = consumer.getOffsetsBefore(req)
      return (metaDataResp,null)
    } catch {
      case e: Exception => return (null,e)
    }
  }
  /**
   *  Previous request failed, need to find a new leader
   */
  private def findNewLeader(oldBroker: String, partitionId: Int): kafka.api.PartitionMetadata = {
    // There are moving parts in Kafka under the failure condtions, we may not have an immediately availabe new
    // leader, so lets try 3 times to get the new leader before bailing
    for (i <- 0 until 3) {
      try {
        val leaderMetaData = findLeader(replicaBrokers.toArray[String], partitionId)
        // Either new metadata leader is not available or the the new broker has not been updated in kafka
        if (leaderMetaData == null || leaderMetaData.leader == null ||
          (leaderMetaData.leader.get.host.equalsIgnoreCase(oldBroker) && i == 0)) {
          Thread.sleep(KafkaSimpleConsumer.SLEEP_DURATION)
        } else {
          return leaderMetaData
        }
      } catch {
        case fae: FatalAdapterException => throw fae
        case e: InterruptedException => {
          val stackTrace = StackTrace.ThrowableTraceString(e)
          LOG.error("Adapter terminated during findNewLeader" + "\nStackTrace:" + stackTrace)
        }
      }
    }
    return null
  }

  /**
   * beginHeartbeat - This adapter will begin monitoring the partitions for the specified topic
   */
  def beginHeartbeat(): Unit = lock.synchronized {
    LOG.debug("Starting monitor for Kafka QUEUE: " + qc.topic)
    startHeartBeat()
  }

  /**
   *  stopHeartbeat - signal this adapter to shut down the monitor thread
   */
  def stopHearbeat(): Unit = lock.synchronized {
    try {
      hbRunning = false
      hbExecutor.shutdownNow()
    } catch {
      case e: java.lang.InterruptedException => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        LOG.debug("Heartbeat terminated" + "\nStackTrace:" + stackTrace)
      }
    }
  }

  /**
   * Private method to start a heartbeat task, and the code that the heartbeat task will execute.....
   * NOT USED YET
   */
  private def startHeartBeat(): Unit = {
    // only start 1 heartbeat
    if (hbRunning) return

    // Block any more heartbeats from being spawned
    hbRunning = true

    // start new heartbeat here.
    hbExecutor.execute(new Runnable() {
      override def run() {
        // Get a connection to each server
        val hbConsumers: Map[String, SimpleConsumer] = Map()
        qc.hosts.foreach(host => {
          val brokerName = host.split(":")
          hbConsumers(host) = new SimpleConsumer(brokerName(0), brokerName(1).toInt,
            KafkaSimpleConsumer.ZOOKEEPER_CONNECTION_TIMEOUT_MS,
            KafkaSimpleConsumer.FETCHSIZE,
            KafkaSimpleConsumer.METADATA_REQUEST_TYPE)
        })

        val topics = Array[String](qc.topic)
        // Get the metadata for each monitored Topic and see if it changed.  If so, notify the engine

        try {
          while (hbRunning) {
            LOG.debug("Heartbeat checking status of " + hbConsumers.size + " broker(s)")
            hbConsumers.foreach {
              case (key, consumer) => {
                val req = new TopicMetadataRequest(topics, KafkaSimpleConsumer.METADATA_REQUEST_CORR_ID)
                val resp: kafka.api.TopicMetadataResponse = consumer.send(req)
                resp.topicsMetadata.foreach(metaTopic => {
                  if (metaTopic.partitionsMetadata.size != hbTopicPartitionNumber) {
                    // TODO: Need to know how to call back to the Engine
                    // first time through the heartbeat
                    if (hbTopicPartitionNumber != -1) {
                      LOG.debug("Partitions changed for TOPIC - " + qc.topic + " on broker " + key + ", it is now" + metaTopic.partitionsMetadata.size)
                    }
                    hbTopicPartitionNumber = metaTopic.partitionsMetadata.size
                  }
                })
              }
            }
            try {
              Thread.sleep(KafkaSimpleConsumer.MONITOR_FREQUENCY)
            } catch {
              case e: java.lang.InterruptedException =>
                val stackTrace = StackTrace.ThrowableTraceString(e)
                LOG.debug("Shutting down the Monitor heartbeat" + "\nStackTrace:" + stackTrace)
                hbRunning = false
            }
          }
        } catch {
          case e: java.lang.Exception => {
            LOG.error("Heartbeat forced down due to exception + ")
          }
        } finally {
          hbConsumers.foreach({ case (key, consumer) => { consumer.close } })
          hbRunning = false
          LOG.debug("Monitor is down")
        }
      }
    })
  }

  /**
   *  Convert the "localhost:XXXX" into an actual IP address.
   */
  private def convertIp(inString: String): String = {
    val brokerName = inString.split(":")
    if (brokerName(0).equalsIgnoreCase("localhost")) {
      brokerName(0) = InetAddress.getLocalHost().getHostAddress()
    }
    val brokerId = brokerName(0) + ":" + brokerName(1)
    brokerId
  }

  /**
   * combine the ip address and port number into a Kafka Configuratio ID
   */
  private def getKafkaConfigId(metadata: kafka.api.PartitionMetadata): String = {
    return metadata.leader.get.host + ":" + metadata.leader.get.port;
  }

  /**
   * terminateHBTasks - Just what it says
   */
  private def terminateHBTasks(): Unit = {
    if (hbExecutor == null) return
    hbExecutor.shutdownNow
    while (hbExecutor.isTerminated == false) {
      Thread.sleep(100) // sleep 100ms and then check
    }
  }

  /**
   *  terminateReaderTasks - well, just what it says
   */
  private def terminateReaderTasks(): Unit = {
    if (readExecutor == null) return

    // Tell all thread to stop processing on the next interval, and shutdown the Excecutor.
    quiesce

    // Give the threads to gracefully stop their reading cycles, and then execute them with extreme prejudice.
    Thread.sleep(qc.noDataSleepTimeInMs + 1)
    readExecutor.shutdownNow
    while (readExecutor.isTerminated == false) {
      Thread.sleep(100)
    }

    LOG.debug("KAFKA_ADAPTER - Shutdown Complete")
    readExecutor = null
    startTime = 0
  }

  /* no need for any synchronization here... it can only go one way.. worst case scenario, a reader thread gets to try to
  *  read the kafka queue one extra time (100ms lost)
   */
  private def quiesce: Unit = {
    isQuiesced = true
  }

}

