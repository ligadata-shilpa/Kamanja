package com.ligadata.InputAdapters

import com.ligadata.HeartBeat.MonitorComponentInfo
import com.ligadata.InputOutputAdapterInfo._
import com.ligadata.AdaptersConfiguration._
import com.ligadata.KamanjaBase.DataDelimiters
import org.apache.logging.log4j.LogManager
import org.json4s.jackson.Serialization

import scala.actors.threadpool.{Executors, ExecutorService}
import scala.collection.mutable.ArrayBuffer


class SmartFileConsumerContext{
  var partitionId: Int = _
  var ignoreFirstMsg: Boolean = _
}

/**
  * Created by Yasser on 3/13/2016.
  */
object SmartFileConsumer extends InputAdapterObj {
  val MONITOR_FREQUENCY = 10000 // Monitor Topic queues every 20 seconds
  val SLEEP_DURATION = 1000 // Allow 1 sec between unsucessful fetched
  var CURRENT_BROKER: String = _
  val FETCHSIZE = 64 * 1024
  val ZOOKEEPER_CONNECTION_TIMEOUT_MS = 3000
  val MAX_TIMEOUT = 60000
  val INIT_TIMEOUT = 250
  val ADAPTER_DESCRIPTION = "Smart File Consumer"

  def CreateInputAdapter(inputConfig: AdapterConfiguration, callerCtxt: InputAdapterCallerContext, execCtxtObj: ExecContextObj, cntrAdapter: CountersAdapter): InputAdapter = new SmartFileConsumer(inputConfig, callerCtxt, execCtxtObj, cntrAdapter)
}

class SmartFileConsumer(val inputConfig: AdapterConfiguration, val callerCtxt: InputAdapterCallerContext, val execCtxtObj: ExecContextObj, cntrAdapter: CountersAdapter) extends InputAdapter {

  val input = this
  lazy val loggerName = this.getClass.getName
  lazy val LOG = LogManager.getLogger(loggerName)

  private val lock = new Object()
  private var readExecutor: ExecutorService = _

  private val adapterConfig = SmartFileAdapterConfiguration.getAdapterConfig(inputConfig)
  private var isShutdown = false
  private var isQuiesced = false
  private var startTime: Long = 0

  private val partitionKVs = scala.collection.mutable.Map[Int, (SmartFilePartitionUniqueRecordKey, SmartFilePartitionUniqueRecordValue, SmartFilePartitionUniqueRecordValue)]()

  private var partitonCounts: collection.mutable.Map[String,Long] = collection.mutable.Map[String,Long]()
  private var metrics: collection.mutable.Map[String,Any] = collection.mutable.Map[String,Any]()
  private var startHeartBeat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(System.currentTimeMillis))
  private var lastSeen: String = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(System.currentTimeMillis))

  val delimiters : DataDelimiters = new DataDelimiters()

  override def Shutdown: Unit = lock.synchronized {
    StopProcessing
  }

  override def DeserializeKey(k: String): PartitionUniqueRecordKey = {
    val key = new SmartFilePartitionUniqueRecordKey
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

  override def DeserializeValue(v: String): PartitionUniqueRecordValue = {
    val vl = new SmartFilePartitionUniqueRecordValue
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

  override def getAllPartitionBeginValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = lock.synchronized {
    getKeyValuePairs()
  }

  override def getAllPartitionEndValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = lock.synchronized {
    getKeyValuePairs()
  }

  private def getKeyValuePairs(): Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = {
    val infoBuffer = ArrayBuffer[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]()

    for(partitionId <- 1 to adapterConfig.monitoringConfig.consumersCount){
      val rKey = new SmartFilePartitionUniqueRecordKey
      val rValue = new SmartFilePartitionUniqueRecordValue

      rKey.PartitionId = partitionId
      rKey.Name = adapterConfig.Name

      rValue.Offset = -1
      rValue.FileName = ""

      infoBuffer.append((rKey, rValue))
    }

    infoBuffer.toArray
  }

  // each value in partitionInfo is (PartitionUniqueRecordKey, PartitionUniqueRecordValue, Long, PartitionUniqueRecordValue). // key, processed value, Start transactionid, Ignore Output Till given Value (Which is written into Output Adapter) & processing Transformed messages (processing & total)
  override def GetAllPartitionUniqueRecordKey: Array[PartitionUniqueRecordKey] = lock.synchronized {

    val infoBuffer = ArrayBuffer[PartitionUniqueRecordKey]()

    for(partitionId <- 1 to adapterConfig.monitoringConfig.consumersCount){
      val rKey = new SmartFilePartitionUniqueRecordKey
      val rValue = new SmartFilePartitionUniqueRecordValue

      rKey.PartitionId = partitionId
      rKey.Name = adapterConfig.Name

      infoBuffer.append(rKey)
    }

    infoBuffer.toArray
  }

  override def StartProcessing(partitionIds: Array[StartProcPartInfo], ignoreFirstMsg: Boolean): Unit = {
    var lastHb: Long = 0
    startHeartBeat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(System.currentTimeMillis))

    LOG.info("START_PROCESSING CALLED")
    // Check to see if this already started
    if (startTime > 0) {
      LOG.error("SMART_FILE_ADAPTER: already started, or in the process of shutting down")
    }
    startTime = System.nanoTime

    if (partitionIds == null || partitionIds.size == 0) {
      LOG.error("SMART_FILE_ADAPTER: Cannot process the kafka queue request, invalid parameters - number")
      return
    }

    val partitionInfoArray = partitionIds.map(quad => {
      (quad._key.asInstanceOf[SmartFilePartitionUniqueRecordKey],
        quad._val.asInstanceOf[SmartFilePartitionUniqueRecordValue],
        quad._validateInfoVal.asInstanceOf[SmartFilePartitionUniqueRecordValue])
    })

    //qc.instancePartitions = partitionInfo.map(partQuad => { partQuad._1.PartitionId }).toSet

    // Make sure the data passed was valid.
    if (partitionInfoArray == null) {
      LOG.error("SMART_FILE_ADAPTER: Cannot process the kafka queue request, invalid parameters - partition instance list")
      return
    }

    val threadsCount =
      if (partitionInfoArray.size == 0)
        1
      else
        partitionInfoArray.size

    readExecutor = Executors.newFixedThreadPool(threadsCount)

    partitionKVs.clear
    partitionInfoArray.foreach(partitionInfo => {
      partitionKVs(partitionInfo._1.PartitionId) = partitionInfo
    })

    delimiters.keyAndValueDelimiter = adapterConfig.keyAndValueDelimiter
    delimiters.fieldDelimiter = adapterConfig.fieldDelimiter
    delimiters.valueDelimiter = adapterConfig.valueDelimiter

    // Enable the adapter to process
    isQuiesced = false
    LOG.debug("SMART_FILE_ADAPTER: Starting " + partitionKVs.size + " threads to process partitions")

    // Schedule a task to perform a read from a give partition.
    partitionKVs.foreach(kvsElement => {
      val partitionId = kvsElement._1
      val partition = kvsElement._2

      var processor = new FileProcessor(kvsElement._1)

      val context = new SmartFileConsumerContext()
      context.partitionId = partitionId
      context.ignoreFirstMsg = ignoreFirstMsg
      processor.init(adapterConfig, context, sendSmartFileMessage)

      readExecutor.execute(processor)



      /*

      // if the offset is -1, then the server wants to start from the begining, else, it means that the server
      // knows what its doing and we start from that offset.
      var readOffset: Long = -1
      val uniqueRecordValue = if (ignoreFirstMsg) partition._3.Offset else partition._3.Offset - 1


      var execThread: ExecContext = null
      val uniqueKey = new SmartFilePartitionUniqueRecordKey
      val uniqueVal = new SmartFilePartitionUniqueRecordValue

      uniqueKey.Name = adapterConfig.Name
      uniqueKey.PartitionId = partitionId

      val readTmNs = System.nanoTime
      val readTmMs = System.currentTimeMillis

      //TODO : when finding a message, must get these values
      val fileName = ""
      val offset = -1
      val message = Array[Byte]()


      // Create a new EngineMessage and call the engine.
      if (execThread == null) {
        execThread = execCtxtObj.CreateExecContext(input, uniqueKey, callerCtxt)
      }

      incrementCountForPartition(partitionId)

      uniqueVal.Offset = offset
      uniqueVal.FileName = fileName
      val dontSendOutputToOutputAdap = uniqueVal.Offset <= uniqueRecordValue

      execThread.execute(message, adapterConfig.formatName, uniqueKey, uniqueVal, readTmNs, readTmMs, dontSendOutputToOutputAdap, adapterConfig.associatedMsg, delimiters)
*/
    })

  }

  private def sendSmartFileMessage(smartMessage : SmartFileMessage,
                                   smartFileConsumerContext: SmartFileConsumerContext): Unit ={

    val partitionId = smartFileConsumerContext.partitionId
    val partition = partitionKVs(partitionId)

    val ignoreFirstMsg = smartFileConsumerContext.ignoreFirstMsg

    // if the offset is -1, then the server wants to start from the begining, else, it means that the server
    // knows what its doing and we start from that offset.
    var readOffset: Long = -1
    val uniqueRecordValue = if (ignoreFirstMsg) partition._3.Offset else partition._3.Offset - 1


    var execThread: ExecContext = null
    val uniqueKey = new SmartFilePartitionUniqueRecordKey
    val uniqueVal = new SmartFilePartitionUniqueRecordValue

    uniqueKey.Name = adapterConfig.Name
    uniqueKey.PartitionId = partitionId

    val readTmNs = System.nanoTime
    val readTmMs = System.currentTimeMillis


    val fileName = smartMessage.relatedFileHandler.getFullPath
    val offset = smartMessage.offsetInFile
    val message = smartMessage.msg


    // Create a new EngineMessage and call the engine.
    if (execThread == null) {
      execThread = execCtxtObj.CreateExecContext(input, uniqueKey, callerCtxt)
    }

    incrementCountForPartition(partitionId)

    uniqueVal.Offset = offset
    uniqueVal.FileName = fileName
    val dontSendOutputToOutputAdap = uniqueVal.Offset <= uniqueRecordValue

    execThread.execute(message, adapterConfig.formatName, uniqueKey, uniqueVal, readTmNs, readTmMs, dontSendOutputToOutputAdap, adapterConfig.associatedMsg, delimiters)

  }

  override def StopProcessing: Unit = {
    isShutdown = true
    terminateReaderTasks
  }

  private def terminateReaderTasks(): Unit = {
    if (readExecutor == null) return

    // Tell all thread to stop processing on the next interval, and shutdown the Excecutor.
    quiesce

    // Give the threads to gracefully stop their reading cycles, and then execute them with extreme prejudice.
    Thread.sleep(adapterConfig.monitoringConfig.waitingTimeMS)
    readExecutor.shutdownNow
    while (readExecutor.isTerminated == false) {
      Thread.sleep(100)
    }

    LOG.debug("Smart File Adapter - Shutdown Complete")
    readExecutor = null
    startTime = 0
  }

  /* no need for any synchronization here... it can only go one way.. worst case scenario, a reader thread gets to try to
*  read  one extra time (100ms lost)
 */
  private def quiesce: Unit = {
    isQuiesced = true
  }

  //TODO : add some data to metrics
  override def getComponentStatusAndMetrics: MonitorComponentInfo = {
    implicit val formats = org.json4s.DefaultFormats

    return new MonitorComponentInfo(AdapterConfiguration.TYPE_INPUT, adapterConfig.Name, SmartFileConsumer.ADAPTER_DESCRIPTION,
      startHeartBeat, lastSeen,  Serialization.write(metrics).toString)
  }

  private def incrementCountForPartition(pid: Int): Unit = {
    var cVal: Long = partitonCounts.getOrElse(pid.toString, 0)
    partitonCounts(pid.toString) = cVal + 1
  }
}