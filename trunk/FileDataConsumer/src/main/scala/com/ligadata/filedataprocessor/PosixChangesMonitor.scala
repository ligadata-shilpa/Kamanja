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

  private var fileObject = new File(fileFullPath)
  private var bufferedReader: BufferedReader = null
  private var in: InputStreamReader = null

  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)

  def this(fullPath : String){
    this()

    fileFullPath = fullPath
  }

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

  @throws(classOf[IOException])
  def openForRead(): Unit = {
    if (isCompressed(fileFullPath)) {
      in = new InputStreamReader(new GZIPInputStream(new FileInputStream(fileFullPath)))
    } else {
      in = new InputStreamReader(new FileInputStream(fileFullPath))
    }
    //bufferedReader = new BufferedReader(in)
  }

  @throws(classOf[IOException])
  def read(buf : Array[Byte], length : Int) : Int = {

    if (bufferedReader == null)
      return -1

    val maxlen = 1000//-------------------------------------
    val cbuffer = new Array[Char](buf.length)
    in.read(cbuffer, 0, length)
  }

  @throws(classOf[IOException])
  def moveTo(newFilePath : String) : Boolean = {
    if(fullPath.equals(newFilePath)){
      logger.warn(s"Trying to move file ($fullPath) but source and destination are the same")
      return false
    }
    try {
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

  //better to move this check into file processor code
  private def isValidFile(fileName: String): Boolean = {
    if (!fileName.endsWith("_COMPLETE"))
      return true
    return false
  }

  //TODO : for now just keep it similar to Dan's code: check only direct child files
  //hdfs and sftp monitors are checking for subfolders actually
  private def checkExistingFiles(d: File): Unit = {
    // Process all the existing files in the directory that are not marked complete.
    if (d.exists && d.isDirectory) {
      val files = d.listFiles.filter(_.isFile).sortWith(_.lastModified < _.lastModified).toList
      files.foreach(file => {
        if (isValidFile(file.toString)) {//file.toString.endsWith(readyToProcessKey) : better to move this check into file processor code
        val tokenName = file.toString.split("/")
          if (!checkIfFileHandled(tokenName(tokenName.size - 1))) {
            logger.info("SMART FILE CONSUMER (global)  Processing " + file.toString)
            //FileProcessor.enQBufferedFile(file.toString)
            val changeType = New
            val fileHandler = new PosixFileHandler(file.toString)
            //call the callback for new files
            modifiedFileCallback(fileHandler, changeType)
          }
        }
      })
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

