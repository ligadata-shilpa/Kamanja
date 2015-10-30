package com.ligadata.filedataprocessor

import java.io.{IOException, File, PrintWriter}
import java.nio.file.StandardCopyOption._
import java.nio.file.{Paths, Files}
import java.text.SimpleDateFormat
import java.util.Properties
import java.util.Date

import com.ligadata.Exceptions._
import com.ligadata.KamanjaBase._
import com.ligadata.MetadataAPI.MetadataAPIImpl
import com.ligadata.Utils.{Utils, KamanjaLoaderInfo}
import com.ligadata.ZooKeeper.CreateClient
import com.ligadata.kamanja.metadata.MdMgr._
import com.ligadata.kamanja.metadata.MessageDef
import kafka.common.{QueueFullException, FailedToSendMessageException}
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import org.apache.curator.framework.CuratorFramework
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer

/**
 * Created by danielkozin on 9/24/15.
 */
class KafkaMessageLoader(partIdx: Int , inConfiguration: scala.collection.mutable.Map[String, String]) {
  var fileBeingProcessing: String = ""
  var numberOfMessagesProcessedInFile: Int = 0
  var startFileProcessingTimeStamp: Long = 0
  var endFileProcessingTimeStamp: Long = 0
  var totalMsgCreationTime: Long = 0
  val RC_RETRY: Int = 3

  var lastOffsetProcessed: Int = 0
  lazy val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  var frmt: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss")

  private var fileCache: scala.collection.mutable.Map[String,Long] = scala.collection.mutable.Map[String,Long]()

  // Set up some properties for the Kafka Producer
  val props = new Properties()
  props.put("metadata.broker.list", inConfiguration(SmartFileAdapterConstants.KAFKA_BROKER));
  props.put("request.required.acks", inConfiguration.getOrElse(SmartFileAdapterConstants.KAFKA_ACK, "1"))
  props.put("batch.num.messages", inConfiguration.getOrElse(SmartFileAdapterConstants.KAFKA_ACK, "200"))

  // create the producer object
  val producer = new Producer[Array[Byte], Array[Byte]](new ProducerConfig(props))

  var delimiters = new DataDelimiters
  delimiters.keyAndValueDelimiter = inConfiguration.getOrElse(SmartFileAdapterConstants.KV_SEPARATOR,"\\x01")
  delimiters.fieldDelimiter = inConfiguration.getOrElse(SmartFileAdapterConstants.FIELD_SEPARATOR,"\\x01")
  delimiters.valueDelimiter = inConfiguration.getOrElse(SmartFileAdapterConstants.VALUE_SEPARATOR,"~")

  var debug_IgnoreKafka = inConfiguration.getOrElse("READ_TEST_ONLY", "FALSE")
  var status_frequency: Int = inConfiguration.getOrElse(SmartFileAdapterConstants.STATUS_FREQUENCY, "100000").toInt
  var isZKIgnore: Boolean = inConfiguration.getOrElse(SmartFileAdapterConstants.ZOOKEEPER_IGNORE, "FALSE").toBoolean

  val zkcConnectString = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZOOKEEPER_CONNECT_STRING")
  logger.debug(partIdx + " SMART FILE CONSUMER Using zookeeper " +zkcConnectString)
  val znodePath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZNODE_PATH") + "/smartFileConsumer/" + partIdx
  var zkc: CuratorFramework = initZookeeper
  var objInst: Any = configureMessageDef
  if (objInst == null) {
    shutdown
    throw new UnsupportedObjectException("Unknown message definition " + inConfiguration(SmartFileAdapterConstants.MESSAGE_NAME))
  }


  /**
   *
   * @param messages
   */
  def pushData(messages: Array[KafkaMessage]): Unit = {
    // First, if we are handling failover, then the messages could be of size 0.
    logger.debug("SMART FILE CONSUMER **** processing chunk of "+messages.size+" messages")
    println("SMART FILE CONSUMER **** processing chunk of "+messages.size+" messages, IGNORE_KAFKA = " + debug_IgnoreKafka + ", IGNORE_ZK = " + isZKIgnore)
    if (messages.size == 0) return

    // If we start processing a new file, then mark so in the zk.
    if (fileBeingProcessing.compareToIgnoreCase(messages(0).relatedFileName) != 0) {
      numberOfMessagesProcessedInFile = 0
      startFileProcessingTimeStamp = 0 //scala.compat.Platform.currentTime
      totalMsgCreationTime = 0
      fileBeingProcessing = messages(0).relatedFileName
      val zkFname = "{" + "\""+fileBeingProcessing+ "\":" + 0 + "}"
      zkc.setData.forPath(znodePath, zkFname.getBytes)
    }

    if (startFileProcessingTimeStamp == 0)
      startFileProcessingTimeStamp = scala.compat.Platform.currentTime

    val keyMessages = new ArrayBuffer[KeyedMessage[Array[Byte], Array[Byte]]](messages.size)

    var numberOfValidEvents: Int = 0
    var numberOfInvalidEvents: Int = 0
    var isLast = false
    var isDummyLast = false

    messages.foreach(msg => {
      if (!msg.isLastDummy) {
        var inputData: InputData = null
        try {
          inputData = CreateKafkaInput(new String(msg.msg), SmartFileAdapterConstants.MESSAGE_NAME, delimiters)
          numberOfMessagesProcessedInFile += 1
        } catch {
          case mfe: KVMessageFormatingException =>
            writeErrorMsg(msg)
            numberOfInvalidEvents += 1
        }

        // Successfully created a message,  add it to the array buffer of crap to sent to kafka
        keyMessages += new KeyedMessage(inConfiguration(SmartFileAdapterConstants.KAFKA_TOPIC),
                                         objInst.asInstanceOf[MessageContainerObjBase].PartitionKeyData(inputData).mkString.getBytes("UTF8"),
                                         new String(msg.msg).getBytes("UTF8"))
        if (msg.isLast) {
          isLast = true
          println("FUCKFUCKFUCK  YEAH  -  LAST")
        }
      } else {
        println("FUCKFUCKFUCK  YEAH  DUMMY LAST")
        isDummyLast = true
      }
    })

    // Push the messages to Kafka
    try {
      if (keyMessages.size > 0) {
        producer.send(keyMessages: _*)
        writeStatusMsg(fileBeingProcessing)
        val zkFname = "{" + "\"" + fileBeingProcessing + "\":" + numberOfValidEvents + "}"
        zkc.setData.forPath(znodePath, zkFname.getBytes)
      }
    } catch {
      case ftsme: FailedToSendMessageException => ftsme.printStackTrace
      case qfe: QueueFullException => qfe.printStackTrace()
      case e: Exception =>
        logger.error(partIdx + " Could not add to the queue due to an Exception " + e.getMessage)
        e.printStackTrace
        shutdown
        throw e
    }

    if (isLast || isDummyLast) {
      // output the status message to the KAFAKA_STATUS_TOPIC
      writeStatusMsg(fileBeingProcessing, true)
      closeOutFile(fileBeingProcessing)
      numberOfMessagesProcessedInFile = 0
      startFileProcessingTimeStamp = 0
    }
  }

  /**
   *
   * @param fileName
   */
  private def closeOutFile (fileName: String): Unit = {
    try {
      println("***************** _ CLOSING " + fileName)
      // Either move or rename the file.
      var fileStruct = fileName.split("/")
      if (inConfiguration.getOrElse(SmartFileAdapterConstants.DIRECTORY_TO_MOVE_TO, null) != null) {
        logger.debug(partIdx + " SMART FILE CONSUMER Moving File" +fileName + " to " + inConfiguration(SmartFileAdapterConstants.DIRECTORY_TO_MOVE_TO))
        Files.copy(Paths.get(fileName), Paths.get(inConfiguration(SmartFileAdapterConstants.DIRECTORY_TO_MOVE_TO) + "/" + fileStruct(fileStruct.size - 1)), REPLACE_EXISTING)
        Files.deleteIfExists(Paths.get(fileName))
      } else {
        logger.debug(partIdx + " SMART FILE CONSUMER Renaming file " + fileName + " to " + fileName + "_COMPLETE")
        (new File(fileName)).renameTo(new File(fileName + "_COMPLETE"))
      }

      println(fileCache)
      fileCache.remove(fileName)
      println(fileCache)
      // SetData in Zookeeper... set null...
      clearRecoveryArea
    } catch {
      case ioe: IOException => ioe.printStackTrace()
    }
  }

  /**
   *
   * @param fileName
   */
  private def writeStatusMsg(fileName: String, isTotal: Boolean = false): Unit = {
    try {
      var cdate: Date = new Date
      if (inConfiguration.getOrElse(SmartFileAdapterConstants.KAFKA_STATUS_TOPIC, "").length > 0) {
        endFileProcessingTimeStamp = scala.compat.Platform.currentTime
        var statusMsg:String = null
        if (!isTotal)
          statusMsg = SmartFileAdapterConstants.KAFKA_LOAD_STATUS + frmt.format(cdate) + "," + fileName + "," + numberOfMessagesProcessedInFile + "," + (endFileProcessingTimeStamp - startFileProcessingTimeStamp)
        else
          statusMsg = SmartFileAdapterConstants.TOTAL_FILE_STATUS + frmt.format(cdate) + "," + fileName + "," + numberOfMessagesProcessedInFile + "," + (endFileProcessingTimeStamp - fileCache(fileName))
        var statusPartitionId = "it does not matter"
        if (debug_IgnoreKafka.equalsIgnoreCase("false")) {
          producer.send(new KeyedMessage(inConfiguration(SmartFileAdapterConstants.KAFKA_STATUS_TOPIC),
            statusPartitionId.getBytes("UTF8"),
            new String(statusMsg).getBytes("UTF8")))
        }

        println("Status pushed ->" + statusMsg)
        logger.debug("Status pushed ->" + statusMsg)
      } else {
        println("NO STATUS Q SPECIFIED")
        logger.debug("NO STATUS Q SPECIFIED")
      }
    } catch {
      case e: Exception => {
        logger.warn(partIdx + " SMART FILE CONSUMER: Unable to externalize status message")
        e.printStackTrace()
      }
    }
  }

  /**
   *
   * @param msg
   */
  private def writeErrorMsg (msg:KafkaMessage) : Unit = {
    var cdate: Date = new Date
    var errorMsg = frmt.format(cdate) + "," + msg.relatedFileName +"," + (new String(msg.msg))
    logger.warn(partIdx + " SMART FILE CONSUMER: invalid message in file "+ msg.relatedFileName)
    println(partIdx + " SMART FILE CONSUMER: invalid message in file "+ msg.relatedFileName)
    if (debug_IgnoreKafka.equalsIgnoreCase("false")) {
      producer.send(new KeyedMessage(inConfiguration(SmartFileAdapterConstants.KAFKA_ERROR_TOPIC),
        "rare event".getBytes("UTF8"),
        errorMsg.getBytes("UTF8")))
    }
  }

  /**
   *
   * @param inputData
   * @param associatedMsg
   * @param delimiters
   * @return
   */
  private def CreateKafkaInput(inputData: String, associatedMsg: String, delimiters: DataDelimiters): InputData = {
    if (associatedMsg == null || associatedMsg.size == 0) {
      shutdown
      throw new Exception("KV data expecting Associated messages as input.")
    }

    if (delimiters.fieldDelimiter == null) delimiters.fieldDelimiter = ","
    if (delimiters.valueDelimiter == null) delimiters.valueDelimiter = "~"
    if (delimiters.keyAndValueDelimiter == null) delimiters.keyAndValueDelimiter = "\\x01"

    val str_arr = inputData.split(delimiters.fieldDelimiter, -1)
    val inpData = new KvData(inputData, delimiters)
    val dataMap = scala.collection.mutable.Map[String, String]()

    if (delimiters.fieldDelimiter.compareTo(delimiters.keyAndValueDelimiter) == 0) {
      if (str_arr.size % 2 != 0) {
        val errStr = "Expecting Key & Value pairs are even number of tokens when FieldDelimiter & KeyAndValueDelimiter are matched. We got %d tokens from input string %s".format(str_arr.size, inputData)
        logger.error(errStr)
        throw new KVMessageFormatingException(errStr)
      }
      for (i <- 0 until str_arr.size by 2) {
        dataMap(str_arr(i).trim) = str_arr(i + 1)
      }
    } else {
      str_arr.foreach(kv => {
        val kvpair = kv.split(delimiters.keyAndValueDelimiter)
        if (kvpair.size != 2) {
          throw new KVMessageFormatingException("Expecting Key & Value pair only")
        }
        dataMap(kvpair(0).trim) = kvpair(1)
      })
    }

    inpData.dataMap = dataMap.toMap
    inpData

  }


  /**
   *
   */
  def initZookeeper: CuratorFramework = {
    try {
      CreateClient.CreateNodeIfNotExists(zkcConnectString, znodePath)
      return CreateClient.createSimple(zkcConnectString)
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.debug("\nStackTrace:" + stackTrace)
        throw new InternalErrorException("Failed to start a zookeeper session with(" + zkcConnectString + "): " + e.getMessage())
      }
    }
  }

  /**
   *
   * @return
   */
  private def configureMessageDef (): com.ligadata.KamanjaBase.BaseMsgObj = {
    val loaderInfo = new KamanjaLoaderInfo()
    var msgDef: MessageDef = null
    try {
      val(typNameSpace, typName) = com.ligadata.kamanja.metadata.Utils.parseNameTokenNoVersion(inConfiguration(SmartFileAdapterConstants.MESSAGE_NAME))
      msgDef = mdMgr.ActiveMessage(typNameSpace, typName)
    } catch {
      case e: Exception => {
        shutdown
        logger.error("Unable to to parse message defintion")
        throw new UnsupportedObjectException("Unknown message definition " + inConfiguration(SmartFileAdapterConstants.MESSAGE_NAME))
      }
    }

    if (msgDef == null) {
      shutdown
      logger.error("Unable to to retrieve message defintion")
      throw new UnsupportedObjectException("Unknown message definition " + inConfiguration(SmartFileAdapterConstants.MESSAGE_NAME))
    }
    // Just in case we want this to deal with more then 1 MSG_DEF in a future.  - msgName paramter will probably have to
    // be an array inthat case.. but for now......
    var allJars = collection.mutable.Set[String]()
    allJars = allJars + msgDef.jarName

    var jarPaths0 = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
    jarPaths0 = jarPaths0 + MetadataAPIImpl.GetMetadataAPIConfig.getProperty("COMPILER_WORK_DIR")

    Utils.LoadJars(allJars.map(j => Utils.GetValidJarFile(jarPaths0, j)).toArray, loaderInfo.loadedJars, loaderInfo.loader)
    val jarName0 = Utils.GetValidJarFile(jarPaths0, msgDef.jarName)
    var classNames = Utils.getClasseNamesInJar(jarName0)


    var tempCurClass: Class[_] = null
    classNames.foreach(clsName => {
      try {
        Class.forName(clsName, true, loaderInfo.loader)
      } catch {
        case e: Exception => {
          logger.error("Failed to load Model class %s with Reason:%s Message:%s".format(clsName, e.getCause, e.getMessage))
          throw e // Rethrow
        }
      }

      var curClz = Class.forName(clsName, true, loaderInfo.loader)
      tempCurClass = curClz

      var isMsg = false
      while (curClz != null && isMsg == false) {
        isMsg = Utils.isDerivedFrom(curClz, "com.ligadata.KamanjaBase.BaseMsgObj")
        if (isMsg == false)
          curClz = curClz.getSuperclass()
      }

      if (isMsg) {
        try {
          // Trying Singleton Object
          val module = loaderInfo.mirror.staticModule(clsName)
          val obj = loaderInfo.mirror.reflectModule(module)
          objInst = obj.instance
          return objInst.asInstanceOf[com.ligadata.KamanjaBase.BaseMsgObj]
        } catch {
          case e: java.lang.NoClassDefFoundError => {
            e.printStackTrace()
            throw e
          }
          case e: Exception => {
            objInst = tempCurClass.newInstance
            return objInst.asInstanceOf[com.ligadata.KamanjaBase.BaseMsgObj]
          }
        }
      }
    })
    return null
  }

  /**
   * checkIfFileBeingProcessed - if for some reason a file name is queued twice... this will prevent it
   * @param file
   * @return
   */
  def checkIfFileBeingProcessed (file: String): Boolean = {
    if (fileCache.contains(file))
      return true
    else {
      fileCache(file) = scala.compat.Platform.currentTime

      return false
    }
  }

  def clearRecoveryArea: Unit = {
    if (zkc != null)
      zkc.setData.forPath(znodePath, null)
    else
      logger.warn("SMART_FILE_CONSUMER " + partIdx + " Unable to connect to Zookeeper")
  }

  /**
   *
   */
  private def shutdown: Unit = {
    MetadataAPIImpl.shutdown
    if (producer != null)
      producer.close
    if (zkc != null)
      zkc.close

    Thread.sleep(2000)
  }
}
