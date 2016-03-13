package com.ligadata.InputAdapters.hdfs

/**
 * Created by Yasser on 12/6/2015.
 */

import java.util.zip.GZIPInputStream

import com.ligadata.Exceptions.KamanjaException
import com.ligadata.InputAdapters.FileChangeType.FileChangeType
import com.ligadata.InputAdapters.FileChangeType._
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.actors.threadpool.{ Executors, ExecutorService }
import java.io.{InputStream}
import com.ligadata.InputAdapters.CompressionUtil._
import org.apache.logging.log4j.{ Logger, LogManager }
import com.ligadata.InputAdapters.{SmartFileHandler, SmartFileMonitor}

class HdfsConnectionConfig(val nameNodeURL: String, val nameNodePort: Int)

class HdfsFileEntry {
  var name : String = ""
  var lastReportedSize : Long = 0
  var lastModificationTime : Long = 0
  //boolean processed
}

class MofifiedFileCallbackHandler(fileHandler : SmartFileHandler, modifiedFileCallback:(SmartFileHandler) => Unit) extends Runnable{
  def run() {
    modifiedFileCallback(fileHandler)
  }
}

class HdfsFileHandler extends SmartFileHandler{

  private var fileFullPath = ""
  
  private var in : InputStream = null
  private var hdFileSystem : FileSystem = null
  private var hdfsConfig : Configuration = null
  private var hdfsConnectionConfig : HdfsConnectionConfig = null

  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)
  
  def this(fullPath : String, config : HdfsConnectionConfig){
    this()

    fileFullPath = fullPath
    hdfsConnectionConfig = config
    hdfsConfig = getHdfsConfig
    hdFileSystem = FileSystem.newInstance(hdfsConfig)
  }

  def getHdfsConfig : Configuration = {
    hdfsConfig = new Configuration()
    hdfsConfig.set("fs.default.name", hdfsConnectionConfig.nameNodeURL + ":" + hdfsConnectionConfig.nameNodePort)
    hdfsConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hdfsConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    hdfsConfig
  }

  /*def this(fullPath : String, fs : FileSystem){
    this(fullPath)

	hdFileSystem = fs
	closeFileSystem = false
  }*/

  def getFullPath = fileFullPath

  @throws(classOf[KamanjaException])
  def openForRead(): Unit = {
    val compressed = isCompressed
    val inFile : Path = new Path(getFullPath)
    hdFileSystem = FileSystem.newInstance(getHdfsConfig)
    in = hdFileSystem.open(inFile)

    if(compressed)
      in = new GZIPInputStream(in)
  }

  @throws(classOf[KamanjaException])
  def read(buf : Array[Byte], length : Int) : Int = {

    try {
      logger.debug("Reading from hdfs file " + fileFullPath)

      if (in == null)
        return -1
      val readLength = in.read(buf, 0, length)
      logger.debug("readLength= " + readLength)
      readLength
    }
    catch{
      case e : Exception => {
        logger.warn("Error while reading from hdfs file [" + fileFullPath + "]",e)
        throw e
      }
    }
  }

  @throws(classOf[KamanjaException])
  def moveTo(newFilePath : String) : Boolean = {
     if(getFullPath.equals(newFilePath)){
      logger.warn(s"Trying to move file ($getFullPath) but source and destination are the same")
      return false
     }

     try {
       hdFileSystem = FileSystem.get(hdfsConfig)
       val srcPath = new Path(getFullPath)
       val destPath = new Path(newFilePath)
       
        if (hdFileSystem.exists(srcPath)) {
          hdFileSystem.rename(srcPath, destPath)
            logger.info("Move remote file success")
            fileFullPath = newFilePath
            return true
        }
        else{
            logger.warn("Source file was not found")
            return false
        }
     } 
     catch {
       case ex : Exception => ex.printStackTrace()
        return false
     } finally {

     }
  }
  
  @throws(classOf[KamanjaException])
  def delete() : Boolean = {
    logger.info(s"Deleting file ($getFullPath)")
     try {
       hdFileSystem = FileSystem.get(hdfsConfig)
       hdFileSystem.delete(new Path(getFullPath), true)
        logger.info("Successfully deleted")
        return true
     } 
     catch {
       case ex : Exception => {
        ex.printStackTrace()
        logger.error(ex.getMessage)
        return false 
       }
        
     } finally {

     }
  }

  @throws(classOf[KamanjaException])
  def close(): Unit = {
    if(in != null)
      in.close()
    if(hdFileSystem != null) {
      logger.debug("Closing Hd File System object hdFileSystem")
      hdFileSystem.close()
    }
  }

  @throws(classOf[KamanjaException])
  def length : Long = {

    try {
      hdFileSystem = FileSystem.get(hdfsConfig)
      hdFileSystem.getFileStatus(new Path(getFullPath)).getLen
    }
    catch {
      case ex : Exception => {
        logger.error(ex.getMessage)
        return 0
      }

    } finally {
    }
  }

  @throws(classOf[KamanjaException])
  def lastModified : Long = {

    try {
      hdFileSystem = FileSystem.get(hdfsConfig)
      hdFileSystem.getFileStatus(new Path(getFullPath)).getModificationTime
    }
    catch {
      case ex : Exception => {
        logger.error(ex.getMessage)
        return -1
      }

    } finally {
    }
  }

  private def isCompressed : Boolean = {

     hdFileSystem = FileSystem.get(hdfsConfig)

      val tempInputStream : FSDataInputStream =
        try {
          val inFile : Path = new Path(getFullPath)
          hdFileSystem.open(inFile)
        }
        catch {
          case e: Exception =>
            logger.error(e)
            null
        }
      val compressed = if(tempInputStream == null) false else isStreamCompressed(tempInputStream)
      try {
        if (tempInputStream != null) {
          tempInputStream.close()
        }

      }
      catch{case e : Exception => {logger.error(e)}
      }
      compressed
    }
}

/**
 * callback is the function to call when finding a modified file, currently has one parameter which is the file path
 */
class HdfsChangesMonitor (modifiedFileCallback:(SmartFileHandler) => Unit) extends SmartFileMonitor{

  private var isMonitoring = false
  
  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)

  private var hdfsConnectionConfig : HdfsConnectionConfig = null

  val poolSize = 5
  private val globalFileMonitorCallbackService: ExecutorService = Executors.newFixedThreadPool(poolSize)

  def init(connectionConfJson: String, monitoringConfJson: String): Unit ={
    /*connectionConf = JsonHelper.getConnectionConfigObj(connectionConfJson)
    monitoringConf = JsonHelper.getMonitoringConfigObj(monitoringConfJson)

    //TODO : validate
    val hostParts = connectionConf.Host.split(":")
    val hostUrl = hostParts(0)
    val port = hostParts(1).toInt
    hdfsConnectionConfig = new HdfsConnectionConfig(hostUrl, port)*/
  }

  def shutdown: Unit ={
    //TODO : use an executor object to run the monitoring and stop here
    isMonitoring = false
  }

  def getFolderContents(parentfolder : String, hdFileSystem : FileSystem) : Array[FileStatus] = {
    try {
      val files = hdFileSystem.listStatus(new Path(parentfolder))
      files
    }
    catch{
      case ex : Exception => {
        logger.error(ex)
        new Array[FileStatus](0)
      }
    }
  }

  def monitor(){
/*
    //TODO : changes this and monitor multi-dirs
    val targetFolder = connectionConf.Locations(0)

    // Instantiate HDFS Configuration.
    val hdfsConfig = new Configuration()
    hdfsConfig.set("fs.default.name", hdfsConnectionConfig.nameNodeURL + ":" + hdfsConnectionConfig.nameNodePort)

    hdfsConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hdfsConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    //hdfsConfig.set("hadoop.job.ugi", "hadoop");//user ???

    val filesStatusMap = Map[String, HdfsFileEntry]()
    var firstCheck = true

    isMonitoring = true

    while(isMonitoring){

      try{
        logger.info(s"Checking configured HDFS directory (targetFolder)...")


        val modifiedDirs = new ArrayBuffer[String]()
        modifiedDirs += targetFolder
        while(modifiedDirs.nonEmpty ){
          //each time checking only updated folders: first find direct children of target folder that were modified
          // then for each folder of these search for modified files and folders, repeat for the modified folders

          val aFolder = modifiedDirs.head
          val modifiedFiles = Map[SmartFileHandler, FileChangeType]() // these are the modified files found in folder $aFolder

          modifiedDirs.remove(0)
          val fs = FileSystem.get(hdfsConfig)
          findDirModifiedDirectChilds(aFolder , fs , filesStatusMap, modifiedDirs, modifiedFiles, firstCheck)

          //logger.debug("Closing Hd File System object fs in monitorDirChanges()")
          //fs.close()

          if(modifiedFiles.nonEmpty)
            modifiedFiles.foreach(tuple =>
            {
                val handler = new MofifiedFileCallbackHandler(tuple._1, modifiedFileCallback)
                 // run the callback in a different thread
                new Thread(handler).start()
                //globalFileMonitorCallbackService.execute(handler)
                //modifiedFileCallback(tuple._1,tuple._2)

            }
            )

        }

      }
      catch{
        case ex: Exception => {
          logger.error(ex.getMessage)
          ex.printStackTrace()
        }
      }

      firstCheck = false

      logger.info(s"Sleepng for ${monitoringConf.WaitingTimeMS} milliseconds...............................")
      Thread.sleep(monitoringConf.WaitingTimeMS)
    }*/

  }

  private def findDirModifiedDirectChilds(parentfolder : String, hdFileSystem : FileSystem, filesStatusMap : Map[String, HdfsFileEntry],
                                          modifiedDirs : ArrayBuffer[String], modifiedFiles : Map[SmartFileHandler, FileChangeType], isFirstCheck : Boolean){
    logger.info("checking folder with full path: " + parentfolder)

    val directChildren = getFolderContents(parentfolder, hdFileSystem)
    var changeType : FileChangeType = null //new, modified

    //process each file reported by FS cache.
    directChildren.foreach(fileStatus => {
      var isChanged = false
      val uniquePath = fileStatus.getPath.toString
      if(!filesStatusMap.contains(uniquePath)){
        //path is new
        isChanged = true
        changeType = if(isFirstCheck) AlreadyExisting else New

        val fileEntry = makeFileEntry(fileStatus)
        filesStatusMap.put(uniquePath, fileEntry)
        if(fileStatus.isDirectory)
          modifiedDirs += uniquePath
      }
      else{
        val storedEntry = filesStatusMap.get(uniquePath).get
        if(fileStatus.getModificationTime >  storedEntry.lastModificationTime){//file has been modified
          storedEntry.lastModificationTime = fileStatus.getModificationTime
          isChanged = true

          changeType = Modified
        }
      }
      
      //TODO : this method to find changed folders is not working as expected. so for now check all dirs
      if(fileStatus.isDirectory)
        modifiedDirs += uniquePath

      if(isChanged){
        if(fileStatus.isDirectory){
          
        }
        else{
          if(changeType == New || changeType == AlreadyExisting) {
            val fileHandler = new HdfsFileHandler(uniquePath, hdfsConnectionConfig)
            modifiedFiles.put(fileHandler, changeType)
          }
        }
      }
    }
    )


    val deletedFiles = new ArrayBuffer[String]()
    filesStatusMap.keys.filter(filePath => isDirectParentDir(filePath, parentfolder)).foreach(pathKey =>
      if(!directChildren.exists(fileStatus => fileStatus.getPath.toString.equals(pathKey))){ //key that is no more in the folder => file/folder deleted
        deletedFiles += pathKey
      }
    )
    deletedFiles.foreach(f => filesStatusMap.remove(f))
  }


  private def isDirectParentDir(file : String, dir : String) : Boolean = {
    try{
      val filePath = new org.apache.hadoop.fs.Path(file)
      val directParentDir = filePath.getParent.toString
      val dirPath = new org.apache.hadoop.fs.Path(dir).toString
      dirPath.toString.equals(directParentDir)
    }
    catch{
      case ex : Exception => false
    }
  }

  private def makeFileEntry(fileStatus : FileStatus) : HdfsFileEntry = {

    val newFile = new HdfsFileEntry()
    newFile.lastReportedSize = fileStatus.getLen
    newFile.name = fileStatus.getPath.toString
    newFile.lastModificationTime = fileStatus.getModificationTime
    newFile
  }

  def stopMonitoring(){
    isMonitoring = false
  }
}