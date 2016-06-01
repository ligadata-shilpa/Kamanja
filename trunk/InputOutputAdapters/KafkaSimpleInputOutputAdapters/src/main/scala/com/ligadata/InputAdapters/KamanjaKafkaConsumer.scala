package com.ligadata.InputAdapters

import java.util
import java.util.Properties

import com.ligadata.AdaptersConfiguration.{KafkaQueueAdapterConfiguration, KafkaPartitionUniqueRecordValue, KafkaPartitionUniqueRecordKey}
import com.ligadata.Exceptions.KamanjaException
import com.ligadata.HeartBeat.MonitorComponentInfo
import com.ligadata.InputOutputAdapterInfo._
import com.ligadata.KamanjaBase.{NodeContext, DataDelimiters}
import kafka.api.{FetchResponse, FetchRequestBuilder}
import kafka.consumer.SimpleConsumer
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.{TopicPartition, PartitionInfo}
import org.apache.logging.log4j.LogManager
import org.json4s.jackson.Serialization

import scala.actors.threadpool.{ExecutorService, Executors}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
  *
  */
object KamanjaKafkaConsumer extends InputAdapterFactory {

  val INITIAL_SLEEP = 500
  val MAX_SLEEP = 30000
  val POLLING_INTERVAL = 100

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
  val input = this
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
  private var sleepDuration = 500
  private var isQuiese = false
  private val kvs = scala.collection.mutable.Map[Int, (KafkaPartitionUniqueRecordKey, KafkaPartitionUniqueRecordValue, KafkaPartitionUniqueRecordValue)]()
  var readExecutor: ExecutorService = _


  private def createConsumerWithInputProperties(): org.apache.kafka.clients.consumer.KafkaConsumer[String,String] = {
    var props = new Properties()
    props.put("bootstrap.servers", qc.hosts.mkString(","))
    props.put("enable.auto.commit", "false")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props
    new org.apache.kafka.clients.consumer.KafkaConsumer[String,String] (props)
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

    LOG.info("Start processing called on KamanjaKafkaAdapter for topic " + qc.topic)
    println("START PROCESSING ON FOR " + qc.topic)

    // This is the number of executors we will run - Heuristic, but will go with it
   // var numberOfThreads = availableThreads
    var numberOfThreads = 2
    readExecutor = Executors.newFixedThreadPool(numberOfThreads)

    // Get the data about the request and set the instancePartition list.
    val partitionInfo = partitionIds.map(quad => {
      (quad._key.asInstanceOf[KafkaPartitionUniqueRecordKey],
        quad._val.asInstanceOf[KafkaPartitionUniqueRecordValue],
        quad._validateInfoVal.asInstanceOf[KafkaPartitionUniqueRecordValue])
    })

    qc.instancePartitions = partitionInfo.map(partQuad => { partQuad._1.PartitionId }).toSet

    var partitionGroups: scala.collection.mutable.Map[Int, scala.collection.mutable.Set[(KafkaPartitionUniqueRecordKey, KafkaPartitionUniqueRecordValue, KafkaPartitionUniqueRecordValue )]]
                         = scala.collection.mutable.Map[Int, scala.collection.mutable.Set[(KafkaPartitionUniqueRecordKey, KafkaPartitionUniqueRecordValue, KafkaPartitionUniqueRecordValue )]]()

    // Create a Map of all the partiotion Ids.  We use a MOD for all the partition numbers to allocate each partition into
    // a bucket.
    kvs.clear
    partitionInfo.foreach(quad => {
      kvs(quad._1.PartitionId) = quad

      var bucketNumber = quad._1.PartitionId % numberOfThreads
      if (partitionGroups.contains(bucketNumber)) {
        partitionGroups(bucketNumber) = partitionGroups(bucketNumber) + quad
      } else {
        partitionGroups(bucketNumber) =  scala.collection.mutable.Set[(KafkaPartitionUniqueRecordKey, KafkaPartitionUniqueRecordValue, KafkaPartitionUniqueRecordValue )](quad)
      }
    })


    // So, now that we have our partition buckets, we start a thread for each buckets.  This involves seeking to the desired location for each partition
    // in the bucket, then starting to POLL.
    partitionGroups.foreach(group => {
      println("Staring Consumers")

      readExecutor.execute(new Runnable() {
        var intSleepTimer = KamanjaKafkaConsumer.INITIAL_SLEEP

        // One consumer to service this group
        var kafkaConnection: org.apache.kafka.clients.consumer.KafkaConsumer[String,String] = null
        // A Map of Execution Contexts to notify.. Assuming that The engine is still going to be running a Learning Engine
        // per partition.
        //var execContexts : scala.collection.mutable.Map[Int, ExecContext] = scala.collection.mutable.Map[Int, ExecContext]()
        //var uniqueVals : scala.collection.mutable.Map[Int, KafkaPartitionUniqueRecordKey] = scala.collection.mutable.Map[Int, KafkaPartitionUniqueRecordKey]()
        //var topicPartitions: scala.collection.mutable.Map[Int, org.apache.kafka.common.TopicPartition] = scala.collection.mutable.Map[Int, org.apache.kafka.common.TopicPartition]()
        //var initialOffsets: scala.collection.mutable.Map[Int, Long] = scala.collection.mutable.Map[Int, Long]()

        var execContexts : ArrayBuffer[ExecContext] = ArrayBuffer[ExecContext]()
        var uniqueVals : ArrayBuffer[KafkaPartitionUniqueRecordKey] = ArrayBuffer[KafkaPartitionUniqueRecordKey]()
        var topicPartitions: ArrayBuffer[org.apache.kafka.common.TopicPartition] = ArrayBuffer[org.apache.kafka.common.TopicPartition]()
        var initialOffsets: ArrayBuffer[Long] = ArrayBuffer[Long]()


        var isSeekSuccessful = false
        private def internalGetSleep : Int = {
          var thisSleep = sleepDuration
          sleepDuration = scala.math.max(KamanjaKafkaConsumer.MAX_SLEEP, thisSleep *2)
          return intSleepTimer
        }

        private def resetSleepTimer : Unit = {
          intSleepTimer =  KamanjaKafkaConsumer.INITIAL_SLEEP
        }

        println("SEEKING....")
        // Create the connection for this thread to use and SEEK the appropriate offsets.
        while (!isSeekSuccessful && !isQuiese) {
          try {
            kafkaConnection = createConsumerWithInputProperties
            var partitionsToMonitor: java.util.List[TopicPartition] = new util.ArrayList[TopicPartition]()

            //intitialize and seek each partition
            group._2.foreach(partitionInfo => {
              var thisPartitionNumber: Int = partitionInfo._1.asInstanceOf[KafkaPartitionUniqueRecordKey].PartitionId
              var thisTopicPartition: org.apache.kafka.common.TopicPartition = null
              LOG.debug("Seeking to requested point for topic " + qc.topic + " for partition " + thisPartitionNumber)

              // Create an execution context for this partition
              if (execContexts.contains(thisPartitionNumber)) {
                LOG.warn("KamanjaKafkaConsumer is creating a duplicate ExecutionContext for partition " + KamanjaKafkaConsumer)
                println("ERROR>>>>>>>>>ERRROR>>>>>>>>>>ERROR")
              } else {
                val uniqueKey = new KafkaPartitionUniqueRecordKey
                thisTopicPartition = new TopicPartition(qc.topic, thisPartitionNumber)
                uniqueKey.Name = qc.Name
                uniqueKey.TopicName = qc.topic
                uniqueKey.PartitionId = thisPartitionNumber

                uniqueVals.insert(thisPartitionNumber, uniqueKey)
                execContexts.insert(thisPartitionNumber, execCtxtObj.CreateExecContext(input, uniqueKey, nodeContext))
                topicPartitions.insert(thisPartitionNumber, thisTopicPartition)
                initialOffsets.insert(thisPartitionNumber, partitionInfo._2.asInstanceOf[KafkaPartitionUniqueRecordValue].Offset.toLong)
              }

              // Seek to the desired offset for this partition
              if (thisTopicPartition != null) partitionsToMonitor.add(thisTopicPartition)
            })
            kafkaConnection.assign(partitionsToMonitor)

            // Ok, the partitions have been assigned, seek all the right offsets
            topicPartitions.foreach(partitionIfno => {
              println("Testing")
              if (initialOffsets(partitionIfno.partition) > 0)
                // TODO Handle exceptions here
                kafkaConnection.seek(topicPartitions(partitionIfno.partition), initialOffsets(partitionIfno.partition))
              else
                kafkaConnection.seekToBeginning(topicPartitions(partitionIfno.partition))
            })
            isSeekSuccessful = true
            resetSleepTimer
          } catch {
            case e: Throwable => {
              LOG.warn("KamanjaKafkaConsumer Exception initializing consumer",e)
              if (kafkaConnection != null) kafkaConnection.close
              kafkaConnection = null
              try {
                Thread.sleep(internalGetSleep)
              } catch {
                case ie: InterruptedException => {
                  LOG.warn("KamanjaKafkaConsumer - sleep interrupted, shutting donw ")
                  throw ie
                }
                case t: Throwable => {
                  throw t
                }
              }

            }
          }
        }

        override def run(): Unit = {
          println("In the RUN method.. polling until turned off.")
          var execContextArray = execContexts.toArray[ExecContext]
          while (!isQuiese) {
            try {
              var records = (kafkaConnection.poll(KamanjaKafkaConsumer.POLLING_INTERVAL)).iterator
              while (records.hasNext) {
                val readTmMs = System.currentTimeMillis

                var record: ConsumerRecord[String, String] = records.next
                val message: Array[Byte] = record.value.getBytes()
                val uniqueVal = new KafkaPartitionUniqueRecordValue
                uniqueVal.Offset = record.offset

                if (execContexts.contains(record.partition)) {
                  execContextArray(record.partition).execute(message,  uniqueVals(record.partition), uniqueVal, readTmMs)
                }
                resetSleepTimer
              }
            } catch {
              case e: Throwable => {
                LOG.warn("KamanjaKafkaConsumer Exception during Kafka Queue processing " + qc.topic + ", cause: ",e)
                try {
                  Thread.sleep(internalGetSleep)
                } catch {
                  case ie: InterruptedException => {
                    LOG.warn("KamanjaKafkaConsumer - sleep interrupted, shutting donw ")
                    throw ie
                  }
                  case t: Throwable => {
                    throw t
                  }
                }
              }
            }
          }

          // Clean up all the existing connections
          try {
            kafkaConnection.close
            kafkaConnection = null
          } catch {
            case e: Throwable => {
              LOG.warn("KamanjaKafkaConsumer Exception trying to close kafka connections ", e)
            }
          }

        }
      })
    })

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
    var kafkaConnection: org.apache.kafka.clients.consumer.KafkaConsumer[String,String] = null
    var isSuccessfulConnection = false

    // Create Consumer object and issue a request for known partitions.
    // per Kamanja design, we will continue trying until we success.  The retry will throttle back to 60 sec
    while (!isSuccessfulConnection && !isQuiese) {
      try {
        kafkaConnection = createConsumerWithInputProperties()
        results = kafkaConnection.partitionsFor(qc.topic)
        isSuccessfulConnection = true
        kafkaConnection.close()
      } catch {
        case e: Throwable => {
          LOG.error ("Exception processing PARTITIONSFOR request..  Retrying ",e)
          try {
            Thread.sleep(getSleepTimer)
          } catch {
            case ie: InterruptedException => {
              LOG.warn("KamanjaKafkaConsumer - sleep interrupted, shutting donw ")
              throw ie
            }
            case t: Throwable => {
              throw t
            }
          }
        }
      }
    }

    resetSleepTimer
    if (isQuiese) {
      // return the info back to the Engine.  if quiesing
      LOG.warn("Quiese request is receive during GetAllPartitionUniqueRecordKey for topic " + qc.topic)
      partitionNames.toArray
    }

    if (results == null)  {
      // return the info back to the Engine.  Just in case we end up with null result
      LOG.warn("Kafka broker returned a null during GetAllPartitionUniqueRecordKey for topic " + qc.topic)
      partitionNames.toArray
    }

    // Successful fetch of metadata.. return the values to the engine.
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
    LOG.warn("Unimplemented method getAllPartitionBeginValues is called on the KamanjaKafkaAdapter for topic " + qc.topic)
    null
  }

  /**
    * Return an array of PartitionUniqueKey/PartitionUniqueRecordValues whre key is the partion and value is the offset
    * within the kafka queue where it eds.
    *
    * @return Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]
    */
  override def getAllPartitionEndValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = lock.synchronized {
    LOG.warn("Unimplemented method getAllPartitionEndValues is called on the KamanjaKafkaAdapter for topic " + qc.topic)
    null
  }

  /**
    *
    */
  override def Shutdown(): Unit = lock.synchronized {
    isQuiese = true
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
    if (readExecutor == null) return

    // Give the threads to gracefully stop their reading cycles, and then execute them with extreme prejudice.
    Thread.sleep(qc.noDataSleepTimeInMs + 1)
    readExecutor.shutdownNow
    while (readExecutor.isTerminated == false) {
      Thread.sleep(100)
    }

    LOG.debug("KAFKA_ADAPTER - Shutdown Complete")
    readExecutor = null
  }

  // This is needed to determine the fist available offset in the broker...  Unfortunately new Consumer API does not support
  // this call anymore..  Should we change the Engien to use commits?
  private def testOldStuff () : Unit = {
    var partConsumer = new SimpleConsumer("localhost", 9092, 3000, (1024 * 1024), KafkaSimpleConsumer.METADATA_REQUEST_TYPE)
    val metaDataResp: kafka.api.TopicMetadataResponse = partConsumer.send(null)
  }

  // Return current sleep timer and double it for the next all.  Max out at MAX_SLEEP (30 secs)...
  // resetSleepTimer will reset this to the original value
  private def getSleepTimer() : Int = {
    var thisSleep = sleepDuration
    sleepDuration = scala.math.max(KamanjaKafkaConsumer.MAX_SLEEP, thisSleep *2)
    return thisSleep
  }

  // Rest the sleep value to the INITIAL_SLEEP
  private def resetSleepTimer(): Unit = {
    sleepDuration = KamanjaKafkaConsumer.INITIAL_SLEEP
  }
}