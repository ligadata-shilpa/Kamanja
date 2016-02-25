package com.ligadata.filedataprocessor

import java.util.zip.{ZipException, GZIPInputStream}
import com.ligadata.Exceptions.{ MissingPropertyException, StackTrace }
import com.ligadata.MetadataAPI.MetadataAPIImpl
import com.ligadata.ZooKeeper.CreateClient
import com.ligadata.kamanja.metadata.MessageDef
import org.apache.curator.framework.CuratorFramework
import org.apache.logging.log4j.{ Logger, LogManager }
import org.json4s.jackson.JsonMethods._
import com.ligadata.kamanja.metadata.MdMgr._
import scala.collection.mutable.HashMap
import scala.collection.JavaConverters._
import util.control.Breaks._
import java.io._
import java.nio.file._
import scala.actors.threadpool.{ Executors, ExecutorService }
import scala.collection.mutable.PriorityQueue
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.Files.copy
import java.nio.file.Paths.get
import scala.collection.mutable.ArrayBuffer
import org.apache.commons.lang3.RandomStringUtils
import java.net.URLEncoder
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.Metadata
import org.apache.tika.detect.Detector
import org.apache.tika.detect.DefaultDetector
import org.apache.tika.mime.MimeTypes
import scala.collection.mutable.Map
import java.net.URLDecoder

case class BufferLeftoversArea(workerNumber: Int, leftovers: Array[Char], relatedChunk: Int)
case class BufferToChunk(len: Int, payload: Array[Char], chunkNumber: Int, relatedFileName: String, firstValidOffset: Int, isEof: Boolean, partMap: scala.collection.mutable.Map[Int,Int])
case class KafkaMessage(msg: Array[Char], offsetInFile: Int, isLast: Boolean, isLastDummy: Boolean, relatedFileName: String, partMap: scala.collection.mutable.Map[Int,Int], msgOffset: Long)
case class EnqueuedFile(name: String, offset: Int, createDate: Long,  partMap: scala.collection.mutable.Map[Int,Int])
case class FileStatus(status: Int, offset: Long, createDate: Long)
case class OffsetValue (lastGoodOffset: Int, partitionOffsets: Map[Int,Int])


/**
 * This is the global area for the File Processor.  It basically handles File Access and Distribution !!!!!
 * the Individual File Processors ask here for what files it they should be processed.
 */
object FileProcessor {
  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)
  private var initRecoveryLock = new Object
  private var numberOfReadyConsumers = 0
  private var isBufferMonitorRunning = false
  private var props: scala.collection.mutable.Map[String, String] = null
  var zkc: CuratorFramework = null
  var znodePath: String = ""
  var localMetadataConfig: String = ""
  var zkcConnectString: String = ""

  //private var path: Path = null
  //Create multiple path watcher
  private var path: ArrayBuffer[Path] = null
  
  private var watchService: WatchService = null
  private var keys = new HashMap[WatchKey, Path]
  private var contentTypes = new scala.collection.mutable.HashMap[String,String]

  var dirToWatch: String = _
  var targetMoveDir: String = _
  var readyToProcessKey: String = _
  
  var globalFileMonitorService: ExecutorService = Executors.newFixedThreadPool(3)
  val DEBUG_MAIN_CONSUMER_THREAD_ACTION = 1000
  val NOT_RECOVERY_SITUATION = -1
  val BROKEN_FILE = -100
  val CORRUPT_FILE = -200
  val REFRESH_RATE = 2000
  val MAX_WAIT_TIME = 60000
  var errorWaitTime = 1000

  var reset_watcher = false

  val KAFKA_SEND_SUCCESS = 0
  val KAFKA_SEND_Q_FULL = 1
  val KAFKA_SEND_DEAD_PRODUCER = 2
  val RECOVERY_DUMMY_START_TIME = 100

  // Possible statuses of files being processed
  val ACTIVE = 0
  val MISSING = 1
  val IN_PROCESS_FAILED = 2
  val FINISHED_FAILED_TO_COPY = 3
  val BUFFERING_FAILED = 4
  var bufferTimeout: Int = 300000  // Default to 5 minutes

  val HEALTHCHECK_TIMEOUT = 30000

  private var fileCache: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()
  private var fileCacheLock = new Object
  // Files in Progress! and their statuses.
  //   Files can be in the following states:
  //    ACTIVE_PROCESSING, BUFFERING, FAILED_TO_MOVE, FAILED_TO_FINISH...
  private var activeFiles: scala.collection.mutable.Map[String, FileStatus] = scala.collection.mutable.Map[String, FileStatus]()
  private val activeFilesLock = new Object
  // READY TO PROCESS FILE QUEUES
  private var fileQ: scala.collection.mutable.PriorityQueue[EnqueuedFile] = new scala.collection.mutable.PriorityQueue[EnqueuedFile]()(Ordering.by(OldestFile))
  private val fileQLock = new Object
  private val bufferingQ_map: scala.collection.mutable.Map[String, Long] = scala.collection.mutable.Map[String, Long]()
  private val bufferingQLock = new Object
  private val zkRecoveryLock = new Object

  /**
   *
   */
  def initZookeeper: CuratorFramework = {
    try {
      zkcConnectString = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZOOKEEPER_CONNECT_STRING")
      logger.info("SMART_FILE_CONSUMER (global) Using zookeeper " + zkcConnectString)
      znodePath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZNODE_PATH") + "/smartFileConsumer"
      CreateClient.CreateNodeIfNotExists(zkcConnectString, znodePath)
      return CreateClient.createSimple(zkcConnectString)
    } catch {
      case e: Exception => {
        logger.error("SMART FILE CONSUMRE (global): unable to connect to zookeeper using " + zkcConnectString, e )
        throw new Exception("Failed to start a zookeeper session with(" + zkcConnectString + "): " + e.getMessage())
      }
    }}

  //
  def addToZK (fileName: String, offset: Int, partitions: scala.collection.mutable.Map[Int,Int] = null) : Unit = {
    zkRecoveryLock.synchronized {
      var zkValue: String = ""
      logger.info("SMART_FILE_CONSUMER (global): Getting zookeeper info for "+ znodePath)
      
      logger.info("SMART_FILE_CONSUMER (MI): addToZK "+ fileName)
      CreateClient.CreateNodeIfNotExists(zkcConnectString, znodePath + "/" + URLEncoder.encode(fileName,"UTF-8"))
      zkValue = zkValue + offset.toString

      // Set up Partition data
      if (partitions == null) {
        zkValue = zkValue + ",[]"
      } else {
        zkValue = zkValue + ",["
        var isFirst = true
        partitions.keySet.foreach(key => {
          if (!isFirst) zkValue = zkValue + ";"
          var mapVal = partitions(key)
          zkValue = zkValue + key.toString + ":" + mapVal.toString
          isFirst = false
        })
        zkValue = zkValue + "]"
      }

      zkc.setData().forPath(znodePath + "/" + URLEncoder.encode(fileName,"UTF-8"), zkValue.getBytes)
    }
  }

  def removeFromZK (fileName: String): Unit = {
    zkRecoveryLock.synchronized {
      try {
        logger.info("SMART_FILE_CONSUMER (global): Removing file " + fileName + " from zookeeper")
        zkc.delete.forPath(znodePath + "/" + URLEncoder.encode(fileName,"UTF-8"))

      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  /**
   * checkIfFileBeingProcessed - if for some reason a file name is queued twice... this will prevent it
   * @param file
   * @return
   */
  def checkIfFileBeingProcessed(file: String): Boolean = {
    fileCacheLock.synchronized {
      if (fileCache.contains(file)) {
        return true
      }
      else {
        fileCache(file) = scala.compat.Platform.currentTime +"::"+RandomStringUtils.randomAlphanumeric(10)
        return false
      }
    }
  }

  def fileCacheRemove(file: String): Unit = {
    fileCacheLock.synchronized {
      fileCache.remove(file)
    }
  }

  def getTimingFromFileCache (file: String): Long = {
    fileCacheLock.synchronized {
      fileCache(file).split("::")(0).toLong
    }
  }
  
   def getIDFromFileCache (file: String): String = {
    fileCacheLock.synchronized {
      fileCache(file)
    }
   }

  //def setProperties(inprops: scala.collection.mutable.Map[String, String], inPath: Path): Unit = {
  def setProperties(inprops: scala.collection.mutable.Map[String, String], inPath: ArrayBuffer[Path]): Unit = {
    props = inprops
    path = inPath
    dirToWatch = props.getOrElse(SmartFileAdapterConstants.DIRECTORY_TO_WATCH, null)
    targetMoveDir = props.getOrElse(SmartFileAdapterConstants.DIRECTORY_TO_MOVE_TO, null)
    readyToProcessKey = props.getOrElse(SmartFileAdapterConstants.READY_MESSAGE_MASK, ".gzip")
  }

  def markFileProcessing (fileName: String, offset: Int, createDate: Long): Unit = {
    activeFilesLock.synchronized{
      logger.info("SMART FILE CONSUMER (global): begin tracking the processing of a file " + fileName + "  begin at offset " + offset)
      var fs = new FileStatus(ACTIVE, offset, createDate)
      activeFiles(fileName) = fs
    }
  }

  def markFileProcessingEnd (fileName: String): Unit = {
    activeFilesLock.synchronized{
      logger.info("SMART FILE CONSUMER: stop tracking the processing of a file " + fileName)
      activeFiles.remove(fileName)
    }
  }

  def setFileState(fileName: String, state: Int): Unit = {
    activeFilesLock.synchronized {
      logger.info("SMART FILE CONSUMER (global): set file state of file " + fileName + " to " + state)
      if (!activeFiles.contains(fileName)) {
        logger.warn("SMART FILE CONSUMER (global): Trying to set state for an unknown file "+ fileName)
      } else {
        val fs = activeFiles(fileName)
        val fs_new = new FileStatus(state, fs.offset, fs.createDate)
        activeFiles(fileName) = fs_new
      }
    }
  }


  // This only applies to the IN_PROCESS_FAILED
  def setFileOffset(fileName: String, offset: Long): Unit = {
    activeFilesLock.synchronized {
      logger.info("SMART FILE CONSUMER (global): changing intermal offset of a file " + fileName + " to " + offset)
      if (!activeFiles.contains(fileName)) {
        logger.warn("SMART FILE CONSUMER (global): Trying to set state for an unknown file "+ fileName)
      } else {
        val fs = activeFiles(fileName)
        val fs_new = new FileStatus(IN_PROCESS_FAILED, scala.math.max(fs.offset,offset), fs.createDate)
        activeFiles(fileName) = fs_new
      }
    }
  }

  def getFileStatus(fileName: String): FileStatus = {
     activeFilesLock.synchronized{
       logger.info("SMART FILE CONSUMER (global): checking file in a list of active files " + fileName)
       if (!activeFiles.contains(fileName)) {
         logger.warn("SMART FILE CONSUMER (global): Trying to get status on unknown file " + fileName)
         return null
       }
       return activeFiles(fileName)
     }
   }



  // Stuff used by the File Priority Queue.
  def OldestFile(file: EnqueuedFile): Long = {
    file.createDate * -1
  }

  private def enQFile(file: String, offset: Int, createDate: Long, partMap: scala.collection.mutable.Map[Int,Int] = scala.collection.mutable.Map[Int,Int]()): Unit = {
    fileQLock.synchronized {
      logger.info("SMART FILE CONSUMER (global):  enq file " + file + " with priority " + createDate+" --- curretnly " + fileQ.size + " files on a QUEUE")
      fileQ += new EnqueuedFile(file, offset, createDate, partMap)
    }
  }

  private def deQFile: EnqueuedFile = {
    fileQLock.synchronized {
      if (fileQ.isEmpty) {
        return null
      }
      val ef = fileQ.dequeue()
      logger.info("SMART FILE CONSUMER (global):  deq file " + ef.name + " with priority " + ef.createDate+" --- curretnly " + fileQ.size + " files left on a QUEUE")
      return ef

    }
  }

  // Code that deals with Buffering Files Queue is below..
  def startGlobalFileMonitor: Unit = {
    // If already started by a different File Consumer - return
    if (isBufferMonitorRunning) return
    isBufferMonitorRunning = true

    logger.info("SMART FILE CONSUMER (global): Initializing global queues")

    // Default to 5 minutes (value given in secopnds
    bufferTimeout = 1000 * props.getOrElse(SmartFileAdapterConstants.FILE_BUFFERING_TIMEOUT,"300").toInt
    localMetadataConfig = props(SmartFileAdapterConstants.METADATA_CONFIG_FILE)
    MetadataAPIImpl.InitMdMgrFromBootStrap(localMetadataConfig, false)
    zkc = initZookeeper

    //watchService = path.getFileSystem().newWatchService()
    //Create a single watchService and register all directories to be watched
    watchService = FileSystems.getDefault().newWatchService()
    
    keys = new HashMap[WatchKey, Path]

    isBufferMonitorRunning = true
    globalFileMonitorService.execute(new Runnable() {
      override def run() = {
        monitorBufferingFiles
      }
    })

    globalFileMonitorService.execute(new Runnable() {
      override def run() = {
        runFileWatcher
      }
    })

    globalFileMonitorService.execute(new Runnable() {
      override def run() = {
        monitorActiveFiles
      }
    })
  }

  private def enQBufferedFile(file: String): Unit = {
    bufferingQLock.synchronized {
      bufferingQ_map(file) = new File(file).length
    }
  }

  /**
   *  Look at the files on the DEFERRED QUEUE... if we see that it stops growing, then move the file onto the READY
   *  to process QUEUE.
   */
  private def monitorBufferingFiles: Unit = {
    // This guys will keep track of when to exgernalize a WARNING Message.  Since this loop really runs every second,
    // we want to throttle the warning messages.
    var specialWarnCounter: Int = 1
    while (true) {
      // Scan all the files that we are buffering, if there is not difference in their file size.. move them onto
      // the FileQ, they are ready to process.
      bufferingQLock.synchronized {
        val iter = bufferingQ_map.iterator
        iter.foreach(fileTuple => {
          try {
            val d = new File(fileTuple._1)
            if (d.exists) {
              // If the the new length of the file is the same as a second ago... this file is done, so move it
              // onto the ready to process q.  Else update the latest length
              if (fileTuple._2 == d.length) {
                if (d.length > 0) {
                  logger.info("SMART FILE CONSUMER (global):  File READY TO PROCESS " + d.toString)
                  enQFile(fileTuple._1, FileProcessor.NOT_RECOVERY_SITUATION, d.lastModified)
                  bufferingQ_map.remove(fileTuple._1)
                } else {
                  var diff = (System.currentTimeMillis - d.lastModified)
                  if (diff > bufferTimeout) {
                    logger.warn("SMART FILE CONSUMER (global): Detected that " + d.toString + " has been on the buffering queue longer then " + bufferTimeout / 1000 + " seconds - Cleaning up" )
                    bufferingQ_map.remove(fileTuple._1)
                    fileCacheRemove(fileTuple._1)
                    moveFile(fileTuple._1)
                  }
                }
              } else {
                bufferingQ_map(fileTuple._1) = d.length
              }
            } else {
              logger.warn("SMART FILE CONSUMER (global): File on the buffering Q is not found " + fileTuple._1)
            }
          } catch {
            case ioe: IOException => {
              logger.error("SMART_FILE_CONSUMER: IOException trying to monitor the buffering queue ",ioe)
            }
          }

        })
      }
      // Give all the files a 1 second to add a few bytes to the contents
      Thread.sleep(2000)
    }
  }

  private def processExistingFiles(d: File): Unit = {
    // Process all the existing files in the directory that are not marked complete.
   
    logger.info("SMART FILE CONSUMER (MI): processExistingFiles on "+d.getAbsolutePath)
    
    if (d.exists && d.isDirectory) {
      val files = d.listFiles.filter(_.isFile).sortWith(_.lastModified < _.lastModified).toList
      files.foreach(file => {
        if(FileProcessor.isValidFile(file.toString)){
          if (!checkIfFileBeingProcessed(file.toString)) {
            FileProcessor.enQBufferedFile(file.toString)
          }
        }else{
          //Invalid File - Move out file and log error
          moveFile(file.toString())
          logger.error("SMART FILE CONSUMER (global): Moving out " + file.toString() + " with invalid file type " )
         }
      })
    }
  }
  
  private def isValidFile(fileName: String): Boolean = {
    logger.info("SMART FILE CONSUMER (MI): isValidFile "+fileName)
    if (fileName.endsWith("_COMPLETE"))
      return false
    else{
      //Sniff only text/plain and application/gzip for now
      var tis = TikaInputStream.get(new File(fileName))
      var metadata = new Metadata
      var detector = new DefaultDetector(MimeTypes.getDefaultMimeTypes())
      var contentType = detector.detect(tis, metadata).toString();
      tis.close()
      
      //Currently handling only text/plain and application/gzip contents
      //Need to bubble this property out into the Constants and Configuration
      if(contentTypes contains contentType){
        return true;
      }else{
        //Log error for invalid content type
        logger.error("SMART FILE CONSUMER (global): Invalid content type " + contentType + " for file "+fileName);
      }
    }
    return false
  }

  private def runFileWatcher(): Unit = {
    try {

      //val d = new File(dirToWatch)

      // Lets see if we have failed previously on this partition Id, and need to replay some messages first.
      logger.info(" SMART FILE CONSUMER (global): Recovery operations, checking  => " + MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZNODE_PATH") + "/smartFileConsumer")
      if (zkc.checkExists().forPath(MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZNODE_PATH") + "/smartFileConsumer") != null) {
        var priorFailures = zkc.getChildren.forPath(MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZNODE_PATH") + "/smartFileConsumer")
        if (priorFailures != null) {
          var map = priorFailures.toArray
          //var map = parse(new String(priorFailures)).values.asInstanceOf[Map[String, Any]]
          if (map != null) map.foreach(fileToReprocess => {
            logger.info("SMART FILE CONSUMER (global): Consumer  recovery of file " + URLDecoder.decode(fileToReprocess.asInstanceOf[String],"UTF-8"))
            
            var fileToRecover = URLDecoder.decode(fileToReprocess.asInstanceOf[String],"UTF-8")
            
            if (!checkIfFileBeingProcessed(fileToRecover)
                //Additional check, see if it exists, possibility that it is moved but not updated in ZK
                //Should we be more particular and check in Processed directory ??? TODO
                && Files.exists(Paths.get(fileToRecover))) {
              
              val offset = zkc.getData.forPath(znodePath + "/" + fileToReprocess.asInstanceOf[String])
              var recoveryInfo = new String(offset)
              logger.info("SMART FILE CONSUMER (global): " + fileToRecover + " from offset " + recoveryInfo)

              // There will always be 2 parts here.
              var partMap = scala.collection.mutable.Map[Int,Int]()
              var recoveryTokens = recoveryInfo.split(",")
              var parts = recoveryTokens(1).substring(1,recoveryTokens(1).size - 1)
              if (parts.size != 0) {
                var kvs = parts.split(";")
                kvs.foreach(kv => {
                  var pair = kv.split(":")
                  partMap(pair(0).toInt) = pair(1).toInt
                })
              }
              
              //Start Changes -- Instead of a single file, run with the ArrayBuffer of Paths
              FileProcessor.enQFile(fileToRecover,recoveryTokens(0).toInt, FileProcessor.RECOVERY_DUMMY_START_TIME, partMap)
              
              for(dir <- path){
                if(dir.toFile().exists() && dir.toFile().isDirectory()){
                  var files = dir.toFile().listFiles.filter(file => { 
                    file.isFile && (file.getName).equals(fileToRecover) 
                  })
                  while (files.size != 0) {
                    Thread.sleep(1000)
                    files = dir.toFile().listFiles.filter(file => { 
                      file.isFile && (file.getName).equals(fileToRecover) 
                    })
                  }
                }
              }
              //End Changes -- Instead of a single file, run with the ArrayBuffer of Paths
            
            }else if(!Files.exists(Paths.get(fileToRecover))){
              //Check if file is already moved, if yes remove from ZK
              val tokenName = fileToRecover.split("/")
              if(Files.exists(Paths.get(targetMoveDir+"/"+tokenName(tokenName.size - 1)))){
                logger.info("SMART FILE CONSUMER (global): Found file " +fileToRecover+" processed ")
                removeFromZK(fileToRecover)
              }
              
            } else {
              logger.info("SMART FILE CONSUMER (global): " +fileToRecover+" already being processed ")
            }
          })
        }
      }

      logger.info("SMART FILE CONSUMER (global): Consumer Continuing Startup process, checking for existing files")
      //processExistingFiles(d)
      for(dir <- path)
        processExistingFiles(dir.toFile())
      

      logger.info("SMART_FILE_CONSUMER partition Initialization complete  Monitoring specified directory for new files")
      // Begin the listening process, TAKE()

      breakable {
        while (true) {
          try {
            //processExistingFiles(d)
            for(dir <- path){
              processExistingFiles(dir.toFile())
            }
            errorWaitTime = 1000
          } catch {
            case e: Exception => {
              logger.warn("Unable to access Directory, Retrying after " + errorWaitTime + " seconds", e)
              errorWaitTime = scala.math.min((errorWaitTime * 2), FileProcessor.MAX_WAIT_TIME)
            }
          }
          Thread.sleep(FileProcessor.REFRESH_RATE)

        }
      }
    }  catch {
      case ie: InterruptedException => logger.error("InterruptedException: " + ie)
      case ioe: IOException         => logger.error("Unable to find the directory to watch, Shutting down File Consumer", ioe)
      case e: Exception             => logger.error("Exception: ", e)
    }
  }


  private def monitorActiveFiles: Unit = {

    var afterErrorConditions = false

    while(true) {
      var isWatchedFileSystemAccesible = true
      var isTargetFileSystemAccesible = true
      var d1: File = null
      logger.info("SMART FILE CONSUMER (global): Scanning for problem files")

      // Watched Directory must be available! NO EXCEPTIONS.. if not, then we need to recreate a file watcher.
      try {
        // DirToWatch is required, TargetMoveDir is  not...
        //d1 = new File(dirToWatch)
        //isWatchedFileSystemAccesible = (d1.canRead && d1.canWrite)
        for(dirName <- dirToWatch.split(System.getProperty("path.separator"))){
          d1 = new File(dirName)
          isWatchedFileSystemAccesible = isWatchedFileSystemAccesible && (d1.canRead && d1.canWrite)
        }
      } catch {
        case fio: IOException => {
          isWatchedFileSystemAccesible = false
        }
      }

      // Target Directory must be available if it is used (it may not be). If it is not, we dont need to restart the watcher
      try {
        var d2: File = null
        if (targetMoveDir != null) {
          d2 = new File(targetMoveDir)
          isTargetFileSystemAccesible = (d2.canRead && d2.canWrite)
        }
      } catch {
        case fio: IOException => {
          isTargetFileSystemAccesible = false
        }
      }


      if (isWatchedFileSystemAccesible && isTargetFileSystemAccesible) {
        logger.info("SMART FILE CONSUMER (global): File system is accessible, perform cleanup for problem files")
        if (afterErrorConditions) {
           try {
            for(dirName <- dirToWatch.split(System.getProperty("path.separator"))){
              d1 = new File(dirName)
              processExistingFiles(d1)
            }
            afterErrorConditions = false
          } catch {
            case e: IOException => {
              // Interesting,  we have just died, and need to move the newly added files to the queue
              logger.warn("SMART FILE CONSUMER (gloabal): Unable to connect to watch directory, will try to shut it down and recreate again")
              watchService.close
              isWatchedFileSystemAccesible = false
              afterErrorConditions = true
            }
          }
        }

        if (isWatchedFileSystemAccesible) {
          val missingFiles = getFailedFiles(MISSING)
          missingFiles.foreach(file => FileProcessor.enQFile(file._1, file._2.asInstanceOf[FileStatus].offset.asInstanceOf[Int], file._2.asInstanceOf[FileStatus].createDate))

          val failedFiles = getFailedFiles(IN_PROCESS_FAILED)
          failedFiles.foreach(file => FileProcessor.enQFile(file._1, file._2.asInstanceOf[FileStatus].offset.asInstanceOf[Int], FileProcessor.RECOVERY_DUMMY_START_TIME))

          val unmovedFiles = getFailedFiles(FINISHED_FAILED_TO_COPY)
          unmovedFiles.foreach(file => completeFile(file._1))
        }
      } else {
        if (!isWatchedFileSystemAccesible) {
          afterErrorConditions = true
          logger.warn("SMART FILE CONSUMER (gloabal): Unable to access a watched directory, will try to shut it down and recreate")
          watchService.close
        }
        logger.warn("SMART FILE CONSUMER (gloabal): File system is not accessible")
      }
      Thread.sleep(HEALTHCHECK_TIMEOUT)
    }

  }


  private def getFailedFiles (fileType: Int): Map[String,FileStatus] = {
    var returnMap: Map[String,FileStatus] = Map[String,FileStatus]()
    activeFilesLock.synchronized {
      val iter = activeFiles.iterator
      while(iter.hasNext) {
        var file = iter.next
        val cStatus = file._2
        if (cStatus.status == fileType) {
          returnMap += file
        }
      }
    }
    if (returnMap.size > 0) {
      logger.warn("SMART FILE CONSUMER (gloabal): Tracking issue for file type " + fileType)
      logger.warn("SMART FILE CONSUMER (gloabal): " + returnMap.toString)
      returnMap.foreach(item => {
        setFileState(item._1, FileProcessor.ACTIVE)
      })
    }
    return returnMap
  }

  // This gets called inthe error case by the recovery logic
  // in normal cases, the KafkaMessafeLoader will handle the completing the file.
  private def completeFile (fileName: String): Unit = {
    try {
      logger.info("SMART FILE CONSUMER {global): - cleaning up after " + fileName)
      // Either move or rename the file.
      moveFile(fileName)
      
      markFileProcessingEnd(fileName)
      fileCacheRemove(fileName)
      removeFromZK(fileName)
    } catch {
      case ioe: IOException => {
        logger.error("Exception moving the file ",ioe)
        FileProcessor.setFileState(fileName,FileProcessor.FINISHED_FAILED_TO_COPY)
      }
    }
  }

  private def moveFile(fileName: String): Unit = {
    val fileStruct = fileName.split("/")
    if (targetMoveDir != null) {
      logger.info("SMART FILE CONSUMER Moving File" + fileName+ " to " + targetMoveDir)
      if(Paths.get(fileName).toFile().exists()){
        Files.copy(Paths.get(fileName), Paths.get(targetMoveDir + "/" + fileStruct(fileStruct.size - 1)), REPLACE_EXISTING)
        Files.deleteIfExists(Paths.get(fileName))
      }else{
        logger.warn("SMART FILE CONSUMER File has been deleted" + fileName);
      }
    } else {
      logger.info("SMART FILE CONSUMER Renaming file " + fileName + " to " + fileName + "_COMPLETE")
      (new File(fileName)).renameTo(new File(fileName + "_COMPLETE"))
    }
  }
  
}


/**
 * Counter of buffers used by the FileProcessors... there is a limit on how much memory File Consumer can use up.
 */
object BufferCounters {
  val inMemoryBuffersCntr = new java.util.concurrent.atomic.AtomicLong()
}

/**
 *
 * @param path
 * @param partitionId
 */
//class FileProcessor(val path: Path, val partitionId: Int) extends Runnable {
class FileProcessor(val path: ArrayBuffer[Path], val partitionId: Int) extends Runnable {
  
  //private var watchService = path.getFileSystem().newWatchService()
  private var watchService = FileSystems.getDefault.newWatchService()
  
  private var keys = new HashMap[WatchKey, Path]
  //private var contentTypes :scala.collection.mutable.HashMap[String,String] = null

  private var kml: KafkaMessageLoader = null
  private var zkc: CuratorFramework = null
  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)
  var fileConsumers: ExecutorService = Executors.newFixedThreadPool(3)

  var isConsuming = true
  var isProducing = true

  private var workerBees: ExecutorService = null
  private var tempKMS = false
  // QUEUES used in file processing... will be synchronized.s
  private var msgQ: scala.collection.mutable.Queue[Array[KafkaMessage]] = scala.collection.mutable.Queue[Array[KafkaMessage]]()
  private var bufferQ: scala.collection.mutable.Queue[BufferToChunk] = scala.collection.mutable.Queue[BufferToChunk]()
  private var blg = new BufferLeftoversArea(-1, null, -1)

  private val msgQLock = new Object
  private val bufferQLock = new Object
  private val beeLock = new Object

  private var msgCount = 0

  // Confugurable Properties
  private var dirToWatch: String = ""
  private var message_separator: Char = _
  private var field_separator: Char = _
  private var kv_separator: Char = _
  private var NUMBER_OF_BEES: Int = 2
  private var maxlen: Int = _
  private var partitionSelectionNumber: Int = _
  private var kafkaTopic = ""
  private var readyToProcessKey = ""
  private var maxBufAllowed: Long  = 0
  private var throttleTime: Int = 0
  private var isRecoveryOps = true

  /**
   * Called by the Directory Listener to initialize
   * @param props
   */
  def init(props: scala.collection.mutable.Map[String, String]): Unit = {
    message_separator = props.getOrElse(SmartFileAdapterConstants.MSG_SEPARATOR, "10").toInt.toChar
    dirToWatch = props.getOrElse(SmartFileAdapterConstants.DIRECTORY_TO_WATCH, null)
    NUMBER_OF_BEES = props.getOrElse(SmartFileAdapterConstants.PAR_DEGREE_OF_FILE_CONSUMER, "1").toInt
    maxlen = props.getOrElse(SmartFileAdapterConstants.WORKER_BUFFER_SIZE, "4").toInt * 1024 * 1024
    partitionSelectionNumber = props(SmartFileAdapterConstants.NUMBER_OF_FILE_CONSUMERS).toInt
    
    //Code commented
    readyToProcessKey = props.getOrElse(SmartFileAdapterConstants.READY_MESSAGE_MASK, ".gzip")
    
    maxBufAllowed = props.getOrElse(SmartFileAdapterConstants.MAX_MEM, "512").toLong * 1024L *1024L
    throttleTime = props.getOrElse(SmartFileAdapterConstants.THROTTLE_TIME, "250").toInt
    var mdConfig = props.getOrElse(SmartFileAdapterConstants.METADATA_CONFIG_FILE,null)
    var msgName = props.getOrElse(SmartFileAdapterConstants.MESSAGE_NAME, null)
    var kafkaBroker = props.getOrElse(SmartFileAdapterConstants.KAFKA_BROKER, null)
    
    //Default allowed content types - 
    var cTypes  = props.getOrElse(SmartFileAdapterConstants.VALID_CONTENT_TYPES, "text/plain;application/gzip")
    
    for(cType <- cTypes.split(";")){
      logger.info("SMART_FILE_CONSUMER Putting "+cType+" into allowed content types")
      if(!FileProcessor.contentTypes.contains(cType))
        FileProcessor.contentTypes.put(cType, cType)
    }
    
    
    kafkaTopic = props.getOrElse(SmartFileAdapterConstants.KAFKA_TOPIC, null)
    
    // Bail out if dirToWatch, Topic are not set
    if (kafkaTopic == null) {
      logger.error("SMART_FILE_CONSUMER ("+partitionId+") Kafka Topic to populate must be specified")
      shutdown
      throw MissingPropertyException("Missing Paramter: " + SmartFileAdapterConstants.KAFKA_TOPIC, null)
    }

    if (dirToWatch == null) {
      logger.error("SMART_FILE_CONSUMER ("+partitionId+") Directory to watch must be specified")
      shutdown
      throw MissingPropertyException("Missing Paramter: " + SmartFileAdapterConstants.DIRECTORY_TO_WATCH, null)
    }

    if (mdConfig == null) {
      logger.error("SMART_FILE_CONSUMER ("+partitionId+") Directory to watch must be specified")
      shutdown
      throw new MissingPropertyException("Missing Paramter: " + SmartFileAdapterConstants.METADATA_CONFIG_FILE, null)
    }

    if (msgName == null) {
      logger.error("SMART_FILE_CONSUMER ("+partitionId+") Message name must be specified")
      shutdown
      throw new MissingPropertyException("Missing Paramter: " + SmartFileAdapterConstants.MESSAGE_NAME, null)
    }

    if (kafkaBroker == null) {
      logger.error("SMART_FILE_CONSUMER ("+partitionId+") Kafka Broker details must be specified")
      shutdown
      throw new MissingPropertyException("Missing Paramter: " + SmartFileAdapterConstants.KAFKA_BROKER, null)
    }
   

    FileProcessor.setProperties(props, path)
    FileProcessor.startGlobalFileMonitor

    logger.info("SMART_FILE_CONSUMER ("+partitionId+") Initializing Kafka loading process")
    // Initialize threads
    try {
      kml = new KafkaMessageLoader(partitionId, props)
    } catch {
      case e: Exception => {
        shutdown
        throw e
      }
    }

    // will need to check zookeeper here
    //val zkcConnectString = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZOOKEEPER_CONNECT_STRING")
   // logger.debug("SMART_FILE_CONSUMER Using zookeeper " + zkcConnectString)
   // val znodePath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZNODE_PATH") + "/smartFileConsumer/" + partitionId
   // zkc = initZookeeper

  }

  private def enQMsg(buffer: Array[KafkaMessage], bee: Int): Unit = {
    msgQLock.synchronized {
      msgQ += buffer
    }
  }

  private def deQMsg(): Array[KafkaMessage] = {
    msgQLock.synchronized {
      if (msgQ.isEmpty) {
        return null
      }
      BufferCounters.inMemoryBuffersCntr.decrementAndGet() // Incrementing when we enQBuffer and Decrementing when we deQMsg
      return msgQ.dequeue
    }
  }

  private def enQBuffer(buffer: BufferToChunk): Unit = {
    bufferQLock.synchronized {
      bufferQ += buffer
    }
  }

  private def deQBuffer(bee: Int): BufferToChunk = {
    msgQLock.synchronized {
      if (bufferQ.isEmpty) {
        return null
      }
      return bufferQ.dequeue
    }
  }

  private def getLeftovers(code: Int): BufferLeftoversArea = {
    beeLock.synchronized {
      return blg
    }
  }

  private def setLeftovers(in: BufferLeftoversArea, code: Int) = {
    beeLock.synchronized {
      blg = in
    }
  }


  /**
   * Each worker bee will run this code... looking for work to do.
   * @param beeNumber
   */
  private def processBuffers(beeNumber: Int) = {

    var msgNum: Int = 0
    var myLeftovers: BufferLeftoversArea = null
    var buffer: BufferToChunk = null;
    var fileNameToProcess: String = ""
    var isEofBuffer = false

    // basically, keep running until shutdown.
    while (isConsuming) {
      var messages: scala.collection.mutable.LinkedHashSet[KafkaMessage] = null
      var leftOvers: Array[Char] = new Array[Char](0)

      // Try to get a new file to process.
      buffer = deQBuffer(beeNumber)

      // If the buffer is there to process, do it
      if (buffer != null) {
        // If the new file being processed, reset offsets to messages in this file to 0.
        if (!fileNameToProcess.equalsIgnoreCase(buffer.relatedFileName)) {
          msgNum = 0
          fileNameToProcess = buffer.relatedFileName
          isEofBuffer = false
        }

        // need a ordered structure to keep the messages.
        messages = scala.collection.mutable.LinkedHashSet[KafkaMessage]()

        var indx = 0
        var prevIndx = indx

        isEofBuffer = buffer.isEof
        if (buffer.firstValidOffset <= FileProcessor.BROKEN_FILE) {
          // Broken File is recoverable, CORRUPTED FILE ISNT!!!!!
          if (buffer.firstValidOffset == FileProcessor.BROKEN_FILE) {
            logger.error("SMART FILE CONSUMER (" + partitionId + "): Detected a broken file")
            messages.add(new KafkaMessage(Array[Char](), FileProcessor.BROKEN_FILE, true, true, buffer.relatedFileName, buffer.partMap, FileProcessor.BROKEN_FILE))
          } else {
            logger.error("SMART FILE CONSUMER (" + partitionId + "): Detected a broken file")
            messages.add(new KafkaMessage(Array[Char](), FileProcessor.CORRUPT_FILE, true, true, buffer.relatedFileName, buffer.partMap, FileProcessor.CORRUPT_FILE))
          }
        } else {
          // Look for messages.
          if (!buffer.isEof){
            buffer.payload.foreach(x => {
              if (x.asInstanceOf[Char] == message_separator) {
                var newMsg: Array[Char] = buffer.payload.slice(prevIndx, indx)
                msgNum += 1
                logger.debug("SMART_FILE_CONSUMER (" + partitionId + ") Message offset " + msgNum + ", and the buffer offset is " + buffer.firstValidOffset)

                // Ok, we could be in recovery, so we have to ignore some messages, but these ignoraable messages must still
                // appear in the leftover areas
                messages.add(new KafkaMessage(newMsg, buffer.firstValidOffset, false, false, buffer.relatedFileName,  buffer.partMap, prevIndx))

                prevIndx = indx + 1
              }
              indx = indx + 1
            })
          }
        }

        // Wait for a previous worker be to finish so that we can get the leftovers.,, If we are the first buffer, then
        // just publish
        if (buffer.chunkNumber == 0) {
          enQMsg(messages.toArray, beeNumber)
        }

        var foundRelatedLeftovers = false
        while (!foundRelatedLeftovers && buffer.chunkNumber != 0) {
          myLeftovers = getLeftovers(beeNumber)
          if (myLeftovers.relatedChunk == (buffer.chunkNumber - 1)) {
            leftOvers = myLeftovers.leftovers
            foundRelatedLeftovers = true

            // Prepend the leftovers to the first element of the array of messages
            val msgArray = messages.toArray
            var firstMsgWithLefovers: KafkaMessage = null
            if (isEofBuffer) {
              if (leftOvers.size > 0) {
                firstMsgWithLefovers = new KafkaMessage(leftOvers, buffer.firstValidOffset, false, false, buffer.relatedFileName, buffer.partMap, buffer.firstValidOffset )
                messages.add(firstMsgWithLefovers)
                enQMsg(messages.toArray, beeNumber)
              }
            } else {
              if (messages.size > 0) {
                firstMsgWithLefovers = new KafkaMessage(leftOvers ++ msgArray(0).msg, msgArray(0).offsetInFile, false, false, buffer.relatedFileName, msgArray(0).partMap, msgArray(0).offsetInFile)
                msgArray(0) = firstMsgWithLefovers
                enQMsg(msgArray, beeNumber)
              }
            }
          } else {
            Thread.sleep(100)
          }
        }

        // whatever is left is the leftover we need to pass to another thread.
        indx = scala.math.min(indx, buffer.len)
        if (indx != prevIndx) {
          if (!isEofBuffer) {
            val newFileLeftOvers = BufferLeftoversArea(beeNumber, buffer.payload.slice(prevIndx, indx), buffer.chunkNumber)
            setLeftovers(newFileLeftOvers, beeNumber)
          } else {
            val newFileLeftOvers = BufferLeftoversArea(beeNumber, new Array[Char](0), buffer.chunkNumber)
            setLeftovers(newFileLeftOvers, beeNumber)
          }

        } else {
          val newFileLeftOvers = BufferLeftoversArea(beeNumber, new Array[Char](0), buffer.chunkNumber)
          setLeftovers(newFileLeftOvers, beeNumber)
        }

      } else {
        // Ok, we did not find a buffer to process on the BufferQ.. wait.
        Thread.sleep(100)
      }
    }
  }

  /**
   * This will be run under a CONSUMER THREAD. - will get called once per consumer thread (from doSomeConsuming)
   * @param file
   */
  private def readBytesChunksFromFile(file: EnqueuedFile): Unit = {

    val buffer = new Array[Char](maxlen)
    var readlen = 0
    var len: Int = 0
    var totalLen = 0
    var chunkNumber = 0

    val fileName = file.name
    val offset = file.offset
    val partMap = file.partMap
    
    logger.info("From readBytesChunksFromFile method - Enqueued File - "+fileName)

    // Start the worker bees... should only be started the first time..
    if (workerBees == null) {
      workerBees = Executors.newFixedThreadPool(NUMBER_OF_BEES)
      for (i <- 1 to NUMBER_OF_BEES) {
        workerBees.execute(new Runnable() {
          override def run() = {
            processBuffers(i)
          }
        })
      }
    }

    // Grab the InputStream from the file and start processing it.  Enqueue the chunks onto the BufferQ for the
    // worker bees to pick them up.
    var bis: BufferedReader = null
    try {
      if (isCompressed(fileName)) {
        bis = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fileName))))
      } else {
        bis = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)))
      }
    } catch {
      case fio: java.io.FileNotFoundException => {

        // Ok, sooo if the file is not Found, either someone moved the file manually, or this specific destination is not reachable..
        // We just drop the file, if it is still in the directory, then it will get picked up and reprocessed the next tick.
        logger.error("SMART_FILE_CONSUMER (" + partitionId + ") Exception accessing the file for processing the file - File is missing",fio)
        FileProcessor.markFileProcessingEnd(fileName)
        FileProcessor.fileCacheRemove(fileName)
        return
      }
      case fio: IOException => {
        logger.error("SMART_FILE_CONSUMER (" + partitionId + ") Exception accessing the file for processing the file ",fio)
        FileProcessor.setFileState(fileName,FileProcessor.MISSING)
        return
      }
    }

    // Intitialize the leftover area for this file reading.
    var newFileLeftOvers = BufferLeftoversArea(0, Array[Char](), -1)
    setLeftovers(newFileLeftOvers, 0)

    var waitedCntr = 0
    var tempFailure = 0
    do {
      waitedCntr = 0
      val st = System.currentTimeMillis
      while ((BufferCounters.inMemoryBuffersCntr.get * 2 + partitionSelectionNumber + 2) * maxlen > maxBufAllowed) { // One counter for bufferQ and one for msgQ and also taken concurrentKafkaJobsRunning and 2 extra in memory
        if (waitedCntr == 0) {
          logger.warn("SMART FILE ADDAPTER (" + partitionId + ") : exceed the allowed memory size (%d) with %d buffers. Halting for free slot".format(maxBufAllowed,
            BufferCounters.inMemoryBuffersCntr.get * 2))
        }
        waitedCntr += 1
        Thread.sleep(throttleTime)
      }

      if (waitedCntr > 0) {
        val timeDiff = System.currentTimeMillis - st
        logger.warn("%d:Got slot after waiting %dms".format(partitionId, timeDiff))
      }

      BufferCounters.inMemoryBuffersCntr.incrementAndGet() // Incrementing when we enQBuffer and Decrementing when we deQMsg
      var isLastChunk = false
      try {
        readlen = bis.read(buffer, 0, maxlen - 1)
        // if (readlen < (maxlen - 1)) isLastChunk = true
      } catch {
        case ze: ZipException => {
          logger.error("Failed to read file, file currupted " + fileName, ze)
          val BufferToChunk = new BufferToChunk(readlen, buffer.slice(0, readlen), chunkNumber, fileName, FileProcessor.CORRUPT_FILE, isLastChunk, partMap)
          enQBuffer(BufferToChunk)
          return
        }
        case ioe: IOException => {
          logger.error("Failed to read file " + fileName, ioe)
          val BufferToChunk = new BufferToChunk(readlen, buffer.slice(0, readlen), chunkNumber, fileName, FileProcessor.BROKEN_FILE, isLastChunk, partMap)
          enQBuffer(BufferToChunk)
          return
        }
        case e: Exception => {
          logger.error("Failed to read file, file corrupted " + fileName, e)
          val BufferToChunk = new BufferToChunk(readlen, buffer.slice(0, readlen), chunkNumber, fileName, FileProcessor.CORRUPT_FILE, isLastChunk, partMap)
          enQBuffer(BufferToChunk)
          return
        }
      }
      if (readlen > 0) {
        totalLen += readlen
        len += readlen
        val BufferToChunk = new BufferToChunk(readlen, buffer.slice(0, readlen), chunkNumber, fileName, offset, isLastChunk, partMap)
        enQBuffer(BufferToChunk)
        chunkNumber += 1
      } else {
        val BufferToChunk = new BufferToChunk(readlen, Array[Char](message_separator), chunkNumber, fileName, offset, true, partMap)
        enQBuffer(BufferToChunk)
        chunkNumber += 1
      }

    } while (readlen > 0)

    // Pass the leftovers..  - some may have been left by the last chunkBuffer... nothing else will pick it up...
    // make it a KamfkaMessage buffer.
    var myLeftovers: BufferLeftoversArea = null
    var foundRelatedLeftovers = false
    while (!foundRelatedLeftovers) {
      myLeftovers = getLeftovers(FileProcessor.DEBUG_MAIN_CONSUMER_THREAD_ACTION)
      // if this is for the last chunk written...
      if (myLeftovers.relatedChunk == (chunkNumber - 1)) {
        // EnqMsg here.. but only if there is something in there.
        if (myLeftovers.leftovers.size > 0) {
          // Not sure how we got there... this should not happen.
          logger.warn("SMART FILE CONSUMER: partition " + partitionId + ": NON-EMPTY final leftovers, this really should not happend... check the file ")
        } else {
          val messages: scala.collection.mutable.LinkedHashSet[KafkaMessage] = scala.collection.mutable.LinkedHashSet[KafkaMessage]()
          messages.add(new KafkaMessage(null, 0, true, true, fileName, scala.collection.mutable.Map[Int,Int](), 0))
          enQMsg(messages.toArray, 1000)
        }
        foundRelatedLeftovers = true
      } else {
        Thread.sleep(100)
      }
      
    }
    // Done with this file... mark is as closed
    try {
      // markFileAsFinished(fileName)
      if (bis != null) bis.close
      bis = null
    } catch {
      case ioe: IOException => {
        logger.warn("SMART FILE CONSUMER: partition " + partitionId + " Unable to detect file as being processed " + fileName)
        logger.warn("SMART FILE CONSUMER: Check to make sure the input directory does not still contain this file " + ioe)
      }
    }
  }

  /**
   *  This is the "FILE CONSUMER"
   */
  private def doSomeConsuming(): Unit = {
    while (isConsuming) {
      val fileToProcess = FileProcessor.deQFile
      var curTimeStart: Long = 0
      var curTimeEnd: Long = 0
      if (fileToProcess == null) {
        Thread.sleep(500)
      } else {
        logger.info("SMART_FILE_CONSUMER partition " + partitionId + " Processing file " + fileToProcess)

        FileProcessor.markFileProcessing(fileToProcess.name, fileToProcess.offset, fileToProcess.createDate)
        
        curTimeStart = System.currentTimeMillis
        try {
          readBytesChunksFromFile(fileToProcess)
        } catch {
          case fnfe: Exception => {
            logger.warn("Exception Encountered, check the logs.",fnfe)
          }
        }
        curTimeEnd = System.currentTimeMillis
      }
    }
  }

  /**
   * This is a "PUSHER" file.
   */
  private def doSomePushing(): Unit = {
    while (isProducing) {
      var msg = deQMsg
      if (msg == null) {
        Thread.sleep(250)
      } else {
        kml.pushData(msg)
        msg = null
      }
    }
  }


  /**
   * The main this will start the Consumer and the Pusher threads.
   */
  override def run(): Unit = {
      // Initialize and launch the File Processor thread(s), and kafka producers
      fileConsumers.execute(new Runnable() {
        override def run() = {
          doSomeConsuming
        }
      })

      fileConsumers.execute(new Runnable() {
        override def run() = {
          doSomePushing
        }
      })
  }

  /**
   * See if this file is compressed.
   * @param inputfile
   * @return
   */
  private def isCompressed(inputfile: String): Boolean = {
    var is: FileInputStream = null
    try {
      is = new FileInputStream(inputfile)
    } catch {
      case fnfe: FileNotFoundException => {
        throw fnfe
      }
      case e: Exception =>
        val stackTrace = StackTrace.ThrowableTraceString(e)
        return false
    }

    val maxlen = 2
    val buffer = new Array[Byte](maxlen)
    val readlen = is.read(buffer, 0, maxlen)

    is.close() // Close before we really check and return the data

    if (readlen < 2)
      return false;

    val b0: Int = buffer(0)
    val b1: Int = buffer(1)

    val head = (b0 & 0xff) | ((b1 << 8) & 0xff00)

    return (head == GZIPInputStream.GZIP_MAGIC);
  }

  /**
   *
   */
  private def shutdown: Unit = {
    isConsuming = false
    isProducing = false
    if (fileConsumers != null) {
      fileConsumers.shutdown()
    }
    MetadataAPIImpl.shutdown
    if (zkc != null)
      zkc.close
    Thread.sleep(2000)
  }
  
   
}


