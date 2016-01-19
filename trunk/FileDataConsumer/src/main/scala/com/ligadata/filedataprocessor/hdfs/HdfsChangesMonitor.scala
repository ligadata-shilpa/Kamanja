package com.ligadata.filedataprocessor.hdfs

/**
 * Created by Yasser on 12/6/2015.
 */

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.actors.threadpool.{ Executors, ExecutorService }
import com.ligadata.filedataprocessor.FileHandler
import java.io.IOException
import com.ligadata.filedataprocessor.FileChangeType._
import org.apache.commons.lang.NotImplementedException

import org.apache.logging.log4j.{ Logger, LogManager }

class HdfsConnectionConfig(val nameNodeURL: String, val nameNodePort: Int)

class HdfsFileEntry {
  var name : String = ""
  var lastReportedSize : Long = 0
  var lastModificationTime : Long = 0
  //boolean processed
}

class MofifiedFileCallbackHandler(fileHandler : FileHandler, fileChangeType : FileChangeType, modifiedFileCallback:(FileHandler, FileChangeType) => Unit) extends Runnable{
  def run() {
    modifiedFileCallback(fileHandler, fileChangeType)
  }
}

class HdfsFileHandler extends FileHandler{

  private var fileFullPath = ""
  def fullPath = fileFullPath
  
  private var in : FSDataInputStream = null
  private var hdFileSystem : FileSystem = null
  private var hdfsConfig : Configuration = null
  private var hdfsConnectionConfig : HdfsConnectionConfig = null

  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)
  
  def this(fullPath : String, config : HdfsConnectionConfig){
    this()

    fileFullPath = fullPath
    hdfsConnectionConfig = config

    hdfsConfig = new Configuration()
    hdfsConfig.set("fs.default.name", hdfsConnectionConfig.nameNodeURL + ":" + hdfsConnectionConfig.nameNodePort)
    hdfsConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hdfsConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    
  }

  /*def this(fullPath : String, fs : FileSystem){
    this(fullPath)

	hdFileSystem = fs
	closeFileSystem = false
  }*/

  @throws(classOf[IOException])
  def openForRead(): Unit = {
    hdFileSystem = FileSystem.get(hdfsConfig)
    
    val inFile : Path = new Path(fullPath)
    in = hdFileSystem.open(inFile)
  }

  @throws(classOf[IOException])
  def read(buf : Array[Byte], length : Int) : Int = {

    if (in == null)
      return -1
    in.read(buf, 0, length)
  }

  @throws(classOf[IOException])
  def moveTo(newFilePath : String) : Boolean = {
      if(fullPath.equals(newFilePath)){
      logger.warn(s"Trying to move file ($fullPath) but source and destination are the same")
      return false
    }
     try {
       hdFileSystem = FileSystem.get(hdfsConfig)
       val srcPath = new Path(fullPath)
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
       if(hdFileSystem!=null)
    	   hdFileSystem.close()
     }
  }
  
  @throws(classOf[IOException])
  def delete() : Boolean = {
    logger.info(s"Deleting file ($fullPath)")
     try {
       hdFileSystem = FileSystem.get(hdfsConfig)
       hdFileSystem.delete(new Path(fullPath), true)
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
       if(hdFileSystem!=null)
    	   hdFileSystem.close()
     }
  }

  @throws(classOf[IOException])
  def close(): Unit = {
    if(in != null)
      in.close()
    if(hdFileSystem != null)
      hdFileSystem.close()
  }

  @throws(classOf[IOException])
  def length : Long = {
    try {
      hdFileSystem = FileSystem.get(hdfsConfig)
      hdFileSystem.getFileStatus(new Path(fullPath)).getLen
    }
    catch {
      case ex : Exception => {
        logger.error(ex.getMessage)
        return 0
      }

    } finally {
      if(hdFileSystem!=null)
        hdFileSystem.close()
    }
  }

  @throws(classOf[IOException])
  def lastModified : Long = {
    try {
      hdFileSystem = FileSystem.get(hdfsConfig)
      hdFileSystem.getFileStatus(new Path(fullPath)).getModificationTime
    }
    catch {
      case ex : Exception => {
        logger.error(ex.getMessage)
        return -1
      }

    } finally {
      if(hdFileSystem!=null)
        hdFileSystem.close()
    }
  }
}

/**
 * callback is the function to call when finding a modified file, currently has one parameter which is the file path
 */
class HdfsChangesMonitor (val hdfsConnectionConfig : HdfsConnectionConfig, val waitingTimeMS : Int,
                          modifiedFileCallback:(FileHandler, FileChangeType) => Unit){

  private var isMonitoring = false
  
  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)

  val poolSize = 5
  private val globalFileMonitorCallbackService: ExecutorService = Executors.newFixedThreadPool(poolSize)

  def getFolderContents(parentfolder : String, hdFileSystem : FileSystem) : Array[FileStatus] = {
    val files = hdFileSystem.listStatus(new Path(parentfolder))
    files
  }

  def monitorDirChanges(targetFolder : String, changeTypesToMonitor : Array[FileChangeType]){

    if(hdfsConnectionConfig == null || hdfsConnectionConfig.nameNodeURL == null || hdfsConnectionConfig.nameNodeURL.trim().length() == 0)
      throw new Exception("Invalid HDFS config params")

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
          val modifiedFiles = Map[FileHandler, FileChangeType]() // these are the modified files found in folder $aFolder

          modifiedDirs.remove(0)
          val fs = FileSystem.get(hdfsConfig)
          findDirModifiedDirectChilds(aFolder , fs , filesStatusMap, modifiedDirs, modifiedFiles, firstCheck)

          if(modifiedFiles.nonEmpty)
            modifiedFiles.foreach(tuple =>
            {
              //get only files with specified change types
              if(changeTypesToMonitor.contains(tuple._2)){
                /*val handler = new MofifiedFileCallbackHandler(tuple._1, tuple._2, modifiedFileCallback)
                 // run the callback in a different thread
                //new Thread(handler).start()
                globalFileMonitorCallbackService.execute(handler)*/
                modifiedFileCallback(tuple._1,tuple._2)
              }
            }
            )
          fs.close()
        }

      }
      catch{
        case ex: Exception => {
          logger.error(ex.getMessage)
          ex.printStackTrace()
        }
      }

      firstCheck = false

      logger.info(s"Sleepng for $waitingTimeMS milliseconds...............................")
      Thread.sleep(waitingTimeMS)
    }

    if(!isMonitoring)
      globalFileMonitorCallbackService.shutdown()
  }

  private def findDirModifiedDirectChilds(parentfolder : String, hdFileSystem : FileSystem, filesStatusMap : Map[String, HdfsFileEntry],
                                          modifiedDirs : ArrayBuffer[String], modifiedFiles : Map[FileHandler, FileChangeType], isFirstCheck : Boolean){
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

          val fileHandler = new HdfsFileHandler(uniquePath, hdfsConnectionConfig)
          modifiedFiles.put(fileHandler, changeType)
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