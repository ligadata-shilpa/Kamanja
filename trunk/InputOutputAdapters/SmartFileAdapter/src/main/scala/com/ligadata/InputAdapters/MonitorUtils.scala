package com.ligadata.InputAdapters

import java.io.{InputStream, IOException, File, FileInputStream}
import java.nio.file.{Paths, Files}

import com.ligadata.AdaptersConfiguration.{SmartFileAdapterConfiguration, FileAdapterMonitoringConfig, FileAdapterConnectionConfig}
import com.ligadata.InputAdapters.hdfs._
import com.ligadata.InputAdapters.sftp._
import com.ligadata.Exceptions.KamanjaException
import net.sf.jmimemagic._
import org.apache.logging.log4j.LogManager
import org.apache.tika.Tika
import org.apache.tika.detect.DefaultDetector

/**
  * Created by Yasser on 3/15/2016.
  */
object MonitorUtils {
  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)


  //Default allowed content types -
  var cTypes  = "text/plain;application/gzip" //might get that from some configuration
  def contentTypes = {
    val map = new scala.collection.mutable.HashMap[String,String]
    for(cType <- cTypes.split(";")){
      map.put(cType, cType)
    }

    map
  }

  def isValidFile(fileHandler: SmartFileHandler): Boolean = {
    //Check if the File exists
    if(fileHandler.exists && fileHandler.length>0) {
      //Sniff only text/plain and application/gzip for now
      var detector = new DefaultDetector()
      var tika = new Tika(detector)
      var is : InputStream = null
      var contentType :String = null

      try {
        is = fileHandler.openForRead()
        contentType = tika.detect(is)
      }catch{
        case e:IOException =>{
          logger.warn("SmartFileConsumer - Tika unable to read from InputStream - "+e.getMessage)
          throw e
        }
        case e:Exception =>{
          logger.warn("SmartFileConsumer - Tika processing generic exception - "+e.getMessage)
          throw e
        }
        case e:Throwable =>{
          logger.warn("SmartFileConsumer - Tika processing runtime exception - "+e.getMessage)
          throw e
        }
      } finally {
        is.close()
      }

      // Register a listener on a watch directory.
      //register(path)


      //val d = new File(dirToWatch)

      //find the best way to handle previously failed file processing considering different fs types

      // Lets see if we have failed previously on this partition Id, and need to replay some messages first.
      if(contentType!= null && !contentType.isEmpty() && contentType.equalsIgnoreCase("application/octet-stream")){
        var magicMatcher : MagicMatch =  null;

        try{

          //read some bytes to pass to getMagicMatch
          is = fileHandler.openForRead()
          val bufferSize = 4096
          val bytes = new Array[Byte](bufferSize)
          fileHandler.read(bytes, bufferSize)

          magicMatcher = Magic.getMagicMatch(bytes, false)
          if(magicMatcher != null)
            contentType = magicMatcher.getMimeType
        }catch{
          case e:MagicParseException =>{
            logger.warn("SmartFileConsumer - MimeMagic caught a parsing exception - "+e.getMessage)
            throw e
          }
          case e:MagicMatchNotFoundException =>{
            logger.warn("SmartFileConsumer -MimeMagic Mime Not Found -"+e.getMessage)
            throw e
          }
          case e:MagicException =>{
            logger.warn("SmartFileConsumer - MimeMagic generic exception - "+e.getMessage)
            throw e
          }
          case e:Exception =>{
            logger.warn("SmartFileConsumer - MimeMagic processing generic exception - "+e.getMessage)
            throw e
          }
          case e:Throwable =>{
            logger.warn("SmartFileConsumer - MimeMagic processing runtime exception - "+e.getMessage)
            throw e
          }

        }
        finally {
          is.close()
        }
      }

      //Currently handling only text/plain and application/gzip contents
      //Need to bubble this property out into the Constants and Configuration
      if(contentTypes contains contentType){
        return true
      }else{
        //Log error for invalid content type
        logger.error("SMART FILE CONSUMER (global): Invalid content type " + contentType + " for file " + fileHandler.getFullPath)
      }
    } else if (!fileHandler.exists) {
      //File doesnot exists - it is already processed
      logger.warn ("SMART FILE CONSUMER (global): File aready processed " + fileHandler.getFullPath)
    } else if (fileHandler.length() == 0 ){
      return true
    }
    return false
  }

  def toCharArray(bytes : Array[Byte]) : Array[Char] = {
    if(bytes == null)
      return null

    bytes.map(b => b.toChar)
  }
}

object SmartFileHandlerFactory{
  def createSmartFileHandler(adapterConfig : SmartFileAdapterConfiguration, fileFullPath : String): SmartFileHandler ={
    val connectionConf = adapterConfig.connectionConfig
    val monitoringConf =adapterConfig.monitoringConfig

    val handler : SmartFileHandler =
      adapterConfig._type.toLowerCase() match {
        case "das/nas" => new PosixFileHandler(fileFullPath)
        case "sftp" => new HdfsFileHandler(fileFullPath, new HdfsConnectionConfig(connectionConf.hostsList.mkString(",")))
        case "hdfs" => new SftpFileHandler(fileFullPath, new SftpConnectionConfig(connectionConf.hostsList(0), connectionConf.userId, connectionConf.password))
        case _ => throw new KamanjaException("Unsupported Smart file adapter type", null)
      }

    handler
  }
}
