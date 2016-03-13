package com.ligadata.InputAdapters.sftp

/**
  * Created by Yasser on 3/10/2016.
  */
import java.io._
import java.util.zip.GZIPInputStream
import com.ligadata.AdaptersConfiguration.{SmartFileAdapterConfiguration, FileAdapterMonitoringConfig, FileAdapterConnectionConfig}
import com.ligadata.Exceptions.{KamanjaException}
import com.ligadata.InputAdapters.FileChangeType.FileChangeType
import com.ligadata.InputAdapters.FileChangeType._
import com.ligadata.InputOutputAdapterInfo.AdapterConfiguration

import scala.collection.mutable.{ArrayBuffer, Map}
import org.apache.commons.vfs2.FileObject
import org.apache.commons.vfs2.FileSystemOptions
import org.apache.commons.vfs2.Selectors
import org.apache.commons.vfs2.impl.StandardFileSystemManager
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder

import com.ligadata.InputAdapters._
import java.net.URLEncoder
import org.apache.logging.log4j.{ Logger, LogManager }
import com.ligadata.InputAdapters.CompressionUtil._


import scala.util.control.Breaks._


class SftpConnectionConfig(val serverAddress: String, val userId: String, val password : String)

object SftpUtility{
  def createConnectionString(sftpConnectionConfig : SftpConnectionConfig, remoteFilePath : String) : String = {
    //"sftp://" + username + ":" + password + "@" + hostName + "/" + remoteFilePath
    "sftp://" + sftpConnectionConfig.userId + ":" + URLEncoder.encode(sftpConnectionConfig.password) +
      "@" + sftpConnectionConfig.serverAddress + "/" +  remoteFilePath
  }

  /**
    * To setup default SFTP config
    */
  def createDefaultOptions() : FileSystemOptions = {
    // Create SFTP options
    val opts = new FileSystemOptions();
    // SSH Key checking
    SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(opts, "no")
    /*
     * Using the following line will cause VFS to choose File System's Root
     * as VFS's root. If I wanted to use User's home as VFS's root then set
     * 2nd method parameter to "true"
     */
    // Root directory set to user home
    SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, false);

    // Timeout is count by Milliseconds
    SftpFileSystemConfigBuilder.getInstance().setTimeout(opts, 10000);

    return opts;
  }


  def hashPath(origianlPathWithConnectionString : String) : String = {
    val colonIndex = origianlPathWithConnectionString.indexOf("://")
    val atIndex = origianlPathWithConnectionString.indexOf("@")

    if(colonIndex < 0 || atIndex < 0  )
      return origianlPathWithConnectionString

    val partToReplace = origianlPathWithConnectionString.substring(colonIndex, atIndex)
    //val hashedPath = origianlPath.replace(partToReplace, "://"+sftpConnectionConfig.userId +":"+"*****")
    val hashedPath = origianlPathWithConnectionString.replace(partToReplace, "://"+"user:pass")
    hashedPath
  }

  def getPathOnly(origianlPathWithConnectionString : String) : String = {
    val atIndex = origianlPathWithConnectionString.indexOf("@")

    if(atIndex < 0  )
      return origianlPathWithConnectionString

    val partToReplace = origianlPathWithConnectionString.substring(0, atIndex + 1)
    val pathWithHost = origianlPathWithConnectionString.replace(partToReplace, "")
    val hostPos = pathWithHost.indexOf("/")
    val pathOnly = pathWithHost.substring(hostPos)
    pathOnly
  }
}
import SftpUtility._

class SftpFileEntry {
  var name : String = ""
  var lastReportedSize : Long = 0
  var lastModificationTime : Long = 0
  var parent : String = ""
  var isDirectory : Boolean = false
}

class SftpFileHandler extends SmartFileHandler{
  private var remoteFullPath = ""

  private var sftpConnectionConfig : SftpConnectionConfig = null
  private var manager : StandardFileSystemManager = null
  private var in : InputStream = null
  //private var bufferedReader : BufferedReader = null

  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)

  def opts = createDefaultOptions
  def sftpEncodedUri =
    if(sftpConnectionConfig == null ) ""
    else "sftp://" + sftpConnectionConfig.userId + ":" + URLEncoder.encode(sftpConnectionConfig.password) +
      "@" + sftpConnectionConfig.serverAddress + "/" +  getFullPath

  def this(path : String, config : SftpConnectionConfig){
    this()
    this.remoteFullPath = path
    sftpConnectionConfig = config
  }

  def getFullPath = remoteFullPath

  @throws(classOf[KamanjaException])
  def openForRead(): Unit = {
    try {
      logger.info(s"Opening SFTP file ($getFullPath) to read")

      manager = new StandardFileSystemManager()
      manager.init()

      val remoteFileObj = manager.resolveFile(sftpEncodedUri, opts)
      in = remoteFileObj.getContent().getInputStream()

      if (isCompressed)
        in = new GZIPInputStream(in)
      //bufferedReader = new BufferedReader(new InputStreamReader(in))
    }
    catch{
      case e : Exception => throw new KamanjaException (e.getMessage, e)
    }
  }

  @throws(classOf[KamanjaException])
  def read(buf : Array[Byte], length : Int) : Int = {
    try {
      if (in == null) {
        logger.warn(s"Trying to read from SFTP file ($getFullPath) but input stream is null")
        return -1
      }
      logger.info(s"Reading from SFTP file ($getFullPath)")
      in.read(buf, 0, length)
    }
    catch{
      case e : Exception => throw new KamanjaException (e.getMessage, e)
    }
  }

  @throws(classOf[KamanjaException])
  def moveTo(remoteNewFilePath : String) : Boolean = {
    if(getFullPath.equals(remoteNewFilePath)){
      logger.warn(s"Trying to move file ($getFullPath) but source and destination are the same")
      return false
    }
    try {
      manager  = new StandardFileSystemManager()
      manager.init()

      val remoteSrcFile = manager.resolveFile(createConnectionString(sftpConnectionConfig, getFullPath), createDefaultOptions())
      val remoteDestFile = manager.resolveFile(createConnectionString(sftpConnectionConfig, remoteNewFilePath), createDefaultOptions())

      if (remoteSrcFile.exists()) {
        remoteSrcFile.moveTo(remoteDestFile)
        logger.info("Move remote file success")
        remoteFullPath = remoteNewFilePath
        return true
      }
      else{
        logger.warn("Source file was not found")
        return false
      }
    }
    catch {
      case ex : Exception => {
        logger.error("", ex)
        return false
      }
    } finally {
      if(manager!=null)
        manager.close()
    }
  }

  @throws(classOf[KamanjaException])
  def delete() : Boolean = {
    logger.info(s"Deleting file ($getFullPath)")
    try {
      manager  = new StandardFileSystemManager()
      manager.init()

      val remoteFile = manager.resolveFile(createConnectionString(sftpConnectionConfig, getFullPath), createDefaultOptions())
      remoteFile.delete()
      logger.info("Successfully deleted")
      return true
    }
    catch {
      case ex : Exception => {
        logger.error(ex.getMessage)
        return false
      }

    } finally {
      if(manager!=null)
        manager.close()
    }
  }

  @throws(classOf[KamanjaException])
  def close(): Unit = {
    logger.info(s"closing SFTP file ($getFullPath)")
    /*if(bufferedReader != null)
      bufferedReader.close()*/
    if(in != null)
      in.close()
    if(manager != null)
      manager.close()
  }

  @throws(classOf[KamanjaException])
  def length : Long = {
    try {
      manager = new StandardFileSystemManager()
      manager.init()
      val remoteFile = manager.resolveFile(createConnectionString(sftpConnectionConfig, getFullPath), createDefaultOptions())
      remoteFile.getContent.getSize
    }
    catch {
      case ex : Exception => {
        logger.error(ex.getMessage)
        return 0
      }

    } finally {
      if(manager!=null)
        manager.close()
    }
  }

  @throws(classOf[KamanjaException])
  def lastModified : Long = {
    try {
      manager = new StandardFileSystemManager()
      manager.init()
      val remoteFile = manager.resolveFile(createConnectionString(sftpConnectionConfig, getFullPath), createDefaultOptions())
      remoteFile.getContent.getLastModifiedTime
    }
    catch {
      case ex : Exception => {
        logger.error(ex.getMessage)
        return -1
      }

    } finally {
      if(manager!=null)
        manager.close()
    }
  }

  private def isCompressed : Boolean = {

    val tempInputStream : InputStream =
      try {
        val remoteFileObj = manager.resolveFile(sftpEncodedUri, opts)
        remoteFileObj.getContent().getInputStream()
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
}

class SftpChangesMonitor (adapterName : String, modifiedFileCallback:(SmartFileHandler) => Unit) extends SmartFileMonitor{

  private var isMonitoring = false
  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)

  private var connectionConf : FileAdapterConnectionConfig = null
  private var monitoringConf :  FileAdapterMonitoringConfig = null

  private var sftpConnectionConfig : SftpConnectionConfig = null

  def init(adapterSpecificCfgJson: String): Unit ={
    val(_type, c, m) =  SmartFileAdapterConfiguration.parseSmartFileAdapterSpecificConfig(adapterName, adapterSpecificCfgJson)
    connectionConf = c
    monitoringConf = m

    if(connectionConf.hostsList == null || connectionConf.hostsList.length == 0){
      val err = "Invalid host for Smart SFTP File Adapter Config:" + adapterName
      throw new KamanjaException(err, null)
    }

    sftpConnectionConfig = new SftpConnectionConfig(connectionConf.hostsList(0), connectionConf.userId, connectionConf.password)
  }

  def monitor: Unit ={

    val manager : StandardFileSystemManager  = new StandardFileSystemManager()

    monitoringConf.locations.foreach(targetRemoteFolder => {
      try{
        //Initializes the file manager
        manager.init();

        //Setup our SFTP configuration
        val opts = createDefaultOptions

        val sftpEncodedUri = createConnectionString(sftpConnectionConfig, targetRemoteFolder)

        val filesStatusMap = Map[String, SftpFileEntry]()
        var firstCheck = true

        isMonitoring = true

        while(isMonitoring){

          try{
            logger.info(s"Checking configured SFTP directory ($targetRemoteFolder)...")

            val modifiedDirs = new ArrayBuffer[String]()
            modifiedDirs += sftpEncodedUri
            while(modifiedDirs.nonEmpty ){
              //each time checking only updated folders: first find direct children of target folder that were modified
              // then for each folder of these search for modified files and folders, repeat for the modified folders

              val aFolder = modifiedDirs.head
              val modifiedFiles = Map[SmartFileHandler, FileChangeType]() // these are the modified files found in folder $aFolder

              modifiedDirs.remove(0)
              findDirModifiedDirectChilds(aFolder, manager,  filesStatusMap, modifiedDirs, modifiedFiles, firstCheck)

              if(modifiedFiles.nonEmpty)
                modifiedFiles.foreach(tuple =>
                {

                  /*val handler = new MofifiedFileCallbackHandler(tuple._1, tuple._2, modifiedFileCallback)
                   // run the callback in a different thread
                  //new Thread(handler).start()
                  globalFileMonitorCallbackService.execute(handler)*/
                  modifiedFileCallback(tuple._1)

                }
                )
            }

          }
          catch{
            case ex: Exception => ex.printStackTrace()
          }

          firstCheck = false

          logger.info(s"Sleepng for ${monitoringConf.waitingTimeMS} milliseconds...............................")
          Thread.sleep(monitoringConf.waitingTimeMS)
        }

        //if(!isMonitoring)
        //globalFileMonitorCallbackService.shutdown()
      }
      catch {
        case ex : Exception => {
          ex.printStackTrace()
        }
      }
      finally {
        manager.close()
      }
    })

  }

  def shutdown: Unit ={
    //TODO : use an executor object to run the monitoring and stop here
    isMonitoring = false

  }


  private def findDirModifiedDirectChilds(parentfolder : String, manager : StandardFileSystemManager, filesStatusMap : Map[String, SftpFileEntry],
                                          modifiedDirs : ArrayBuffer[String], modifiedFiles : Map[SmartFileHandler, FileChangeType], isFirstCheck : Boolean){
    val parentfolderHashed = hashPath(parentfolder)//used for logging since path contains user and password
    logger.info("checking folder with full path: " + parentfolderHashed)

    val directChildren = getRemoteFolderContents(parentfolder, manager)
    var changeType : FileChangeType = null //new, modified

    //process each file reported by FS cache.
    directChildren.foreach(child => {
      val currentChildEntry = makeFileEntry(child)
      var isChanged = false
      val uniquePath = child.getURL().toString()

      if(!filesStatusMap.contains(uniquePath)){
        //path is new
        isChanged = true
        changeType = if(isFirstCheck) AlreadyExisting else New

        filesStatusMap.put(uniquePath, currentChildEntry)
        if(currentChildEntry.isDirectory)
          modifiedDirs += uniquePath
      }
      else{
        val storedEntry = filesStatusMap.get(uniquePath).get
        if(currentChildEntry.lastModificationTime >  storedEntry.lastModificationTime){//file has been modified
          storedEntry.lastModificationTime = currentChildEntry.lastModificationTime
          isChanged = true

          changeType = Modified
        }
      }

      //TODO : this method to find changed folders is not working as expected. so for now check all dirs
      if(currentChildEntry.isDirectory)
        modifiedDirs += uniquePath

      if(isChanged){
        if(currentChildEntry.isDirectory){

        }
        else{
          if(changeType == New || changeType == AlreadyExisting) {
            val fileHandler = new SftpFileHandler(getPathOnly(uniquePath), sftpConnectionConfig)
            modifiedFiles.put(fileHandler, changeType)
          }
        }
      }
    }
    )


    val deletedFiles = new ArrayBuffer[String]()
    /*filesStatusMap.keys.filter(filePath => isDirectParentDir(filePath, parentfolder)).foreach(pathKey =>
      if(!directChildren.exists(fileStatus => fileStatus.getURL().toString.equals(pathKey))){ //key that is no more in the folder => file/folder deleted
        deletedFiles += pathKey
      }
    )*/

    filesStatusMap.values.foreach(fileEntry =>{
      if(isDirectParentDir(fileEntry, parentfolder)){
        if(!directChildren.exists(fileStatus => fileStatus.getURL().toString.equals(fileEntry.name))) //key that is no more in the folder => file/folder deleted
          deletedFiles += fileEntry.name
      }
    })

    deletedFiles.foreach(f => filesStatusMap.remove(f))
  }

  private def getRemoteFolderContents(parentRemoteFolderUri : String, manager : StandardFileSystemManager) : Array[FileObject] = {
    val remoteDir : FileObject = manager.resolveFile(parentRemoteFolderUri )
    val children = remoteDir.getChildren()
    children
  }

  private def makeFileEntry(fileObject : FileObject) : SftpFileEntry = {

    val newFile = new SftpFileEntry()
    newFile.name = fileObject.getURL.toString
    newFile.parent = fileObject.getParent.getURL.toString
    newFile.lastModificationTime = fileObject.getContent.getLastModifiedTime
    newFile.isDirectory = fileObject.getType.getName.equalsIgnoreCase("folder")
    newFile.lastReportedSize = if(newFile.isDirectory) -1 else fileObject.getContent.getSize //size is not defined for folders
    newFile
  }

  /*private def isDirectParentDir(fileObj : FileObject, dir : String) : Boolean = {
    fileObj.getParent.getURL.toString().equals(dir)
  }*/

  private def isDirectParentDir(fileObj : SftpFileEntry, dirUrl : String) : Boolean = {
    fileObj.parent.equals(dirUrl)
  }

}