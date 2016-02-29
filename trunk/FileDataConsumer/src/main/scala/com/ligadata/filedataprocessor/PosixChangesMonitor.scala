package com.ligadata.filedataprocessor

import java.io._
import java.nio.file.Path
import java.nio.file._
import java.util.zip.GZIPInputStream
import com.ligadata.Exceptions.StackTrace
import com.ligadata.filedataprocessor.FileChangeType._
import org.apache.logging.log4j.{ Logger, LogManager }

import scala.collection.mutable.HashMap
import scala.util.control.Breaks._

/**
  * Created by Yasser on 1/14/2016.
  *
  * POSIX file systems directory monitor and necessary classes
  * based on Dan's code
  */


class PosixFileHandler extends FileHandler{

  private var fileFullPath = ""
  def fullPath = fileFullPath

  def fileObject = new File(fileFullPath)
  private var bufferedReader: BufferedReader = null
  //private var in: InputStreamReader = null
  private var in: InputStream = null

  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)

  def this(fullPath : String){
    this()

    fileFullPath = fullPath
  }

  private def isCompressed: Boolean = {

    val tempInputStream : InputStream =
      try {
        new FileInputStream(fileFullPath)
      }
      catch {
        case e: Exception =>
          logger.error(e)
          null
      }
    val compressed = if(tempInputStream == null) false else isStreamCompressed(tempInputStream)
    if(tempInputStream != null){
      try{
        tempInputStream.close()
      }
      catch{case e : Exception => }
    }
    compressed
  }

  @throws(classOf[IOException])
  def openForRead(): Unit = {

    if (isCompressed) {
      in = new GZIPInputStream(new FileInputStream(fileFullPath))
    } else {
      in = new FileInputStream(fileFullPath)
    }
    //bufferedReader = new BufferedReader(in)
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
      logger.info(s"PosixFileHandler - Moving file ${fileObject.toString} to ${newFilePath}")
      val destFileObj = new File(newFilePath)

      if (fileObject.exists()) {
        fileObject.renameTo(destFileObj)
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
      case ex : Exception => logger.error(ex.getMessage)
        return false
    }
  }

  @throws(classOf[IOException])
  def delete() : Boolean = {
    logger.info(s"Deleting file ($fullPath)")
    try {
      fileObject.delete
      logger.info("Successfully deleted")
      return true
    }
    catch {
      case ex : Exception => {
        ex.printStackTrace()
        logger.error(ex.getMessage)
        return false
      }

    }
  }

  @throws(classOf[IOException])
  def length : Long = fileObject.length

  def lastModified : Long = fileObject.lastModified

  @throws(classOf[IOException])
  def close(): Unit = {
    if(in != null)
      in.close()
  }

}


//TODO : create a trait for dir monitors and change sfp, hdfs and posix monitors to extend it
class PosixChangesMonitor(val REFRESH_RATE : Int, modifiedFileCallback:(FileHandler, FileChangeType) => Unit) {

  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)

  private var watchService: WatchService = null
  private var keys = new HashMap[WatchKey, Path]

  private var errorWaitTime = 1000
  val MAX_WAIT_TIME = 60000

  private var fileCache: scala.collection.mutable.Map[String, Long] = scala.collection.mutable.Map[String, Long]()
  private var fileCacheLock = new Object

  def monitorDirChanges(targetFolder : String, changeTypesToMonitor : Array[FileChangeType]): Unit ={
    try{
      breakable {
        while (true) {
          try {
            logger.info(s"Watching directory $targetFolder")
            val dir = new File(targetFolder)
            checkExistingFiles(dir)
            errorWaitTime = 1000
          } catch {
            case e: Exception => {
              logger.warn("Unable to access Directory, Retrying after " + errorWaitTime + " seconds", e)
              errorWaitTime = scala.math.min((errorWaitTime * 2), MAX_WAIT_TIME)
            }
          }
          Thread.sleep(REFRESH_RATE)

        }
      }
    }  catch {
      case ie: InterruptedException => logger.error("InterruptedException: " + ie)
      case ioe: IOException         => logger.error("Unable to find the directory to watch, Shutting down File Consumer", ioe)
      case e: Exception             => logger.error("Exception: ", e)
    }
  }


  //TODO : for now just keep it similar to Dan's code: check only direct child files
  //hdfs and sftp monitors are checking for subfolders actually
  private def checkExistingFiles(d: File): Unit = {
    // Process all the existing files in the directory that are not marked complete.
    if (d.exists && d.isDirectory) {
      val files = d.listFiles.filter(_.isFile).sortWith(_.lastModified < _.lastModified).toList
      files.foreach(file => {
        val tokenName = file.toString.split("/")
          if (!checkIfFileHandled(tokenName(tokenName.size - 1))) {
            logger.info("SMART FILE CONSUMER (global)  Processing " + file.toString)
            //FileProcessor.enQBufferedFile(file.toString)
            val changeType = New
            val fileHandler = new PosixFileHandler(file.toString)
            //call the callback for new files
            logger.info(s"A new file found ${fileHandler.fullPath}")
            modifiedFileCallback(fileHandler, changeType)
          }
      })
    }
    else{
      logger.warn(d.toString + " is not a directory or does not exist")
    }
  }

  /**
    * checkIfFileHandled: previously checkIfFileBeingProcessed - if for some reason a file name is queued twice... this will prevent it
    * @param file
    * @return
    */
  def checkIfFileHandled(file: String): Boolean = {
    fileCacheLock.synchronized {
      if (fileCache.contains(file)) {
        return true
      }
      else {
        fileCache(file) = scala.compat.Platform.currentTime
        return false
      }
    }
  }


}

