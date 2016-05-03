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

import scala.collection.mutable.{ArrayBuffer, Map}
import org.apache.commons.vfs2.{FileType, FileObject, FileSystemOptions, Selectors}
import org.apache.commons.vfs2.impl.StandardFileSystemManager
import com.ligadata.InputAdapters._
import org.apache.logging.log4j.{ Logger, LogManager }
import SftpUtility._
import scala.actors.threadpool.{Executors, ExecutorService}
import com.jcraft.jsch._


class SftpFileEntry {
  var name : String = ""
  var lastReportedSize : Long = 0
  var lastModificationTime : Long = 0
  var parent : String = ""
  var isDirectory : Boolean = false
}

class SftpFileHandler extends SmartFileHandler{
  private var remoteFullPath = ""

  private var connectionConfig : FileAdapterConnectionConfig = null
  //private var manager : StandardFileSystemManager = null
  private var in : InputStream = null
  //private var bufferedReader : BufferedReader = null

  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)

  def opts = createDefaultOptions(connectionConfig)
  def sftpEncodedUri = SftpUtility.createConnectionString(connectionConfig, getFullPath)

  private var passphrase : String = null

  private var host : String = _
  private var port : Int = _
  private var channelSftp : ChannelSftp = null
  private var session : Session = null
  private var jsch : JSch = null
  def getNewSession = jsch.getSession(connectionConfig.userId, host, port)

  def this(path : String, config : FileAdapterConnectionConfig){
    this()
    this.remoteFullPath = path
    connectionConfig = config

    passphrase = if (connectionConfig.keyFile != null && connectionConfig.keyFile.length > 0)
      connectionConfig.passphrase else null

    val hostTokens = connectionConfig.hostsList(0).split(":")
    host = hostTokens(0)
    port = if(hostTokens(1) != null && hostTokens(1).length >0 ) hostTokens(1).toInt else 22 //default

    jsch = new JSch()
    if (connectionConfig.keyFile != null && connectionConfig.keyFile.length > 0) {
      jsch.addIdentity(connectionConfig.keyFile)
    }
  }

  def getFullPath = remoteFullPath

  //gets the input stream according to file system type - SFTP here
  def getDefaultInputStream() : InputStream = {

    val ui=new SftpUserInfo(connectionConfig.password, passphrase)

    session = getNewSession
    session.setUserInfo(ui)
    session.connect()
    val channel = session.openChannel("sftp");
    channel.connect()
    channelSftp = channel.asInstanceOf[ChannelSftp];

    val inputStream : InputStream =
      try {

        channelSftp.get(remoteFullPath)
      }
      catch {
        case e: Exception => {
          logger.error("Error getting input stream for file " + getFullPath, e)
          null
        }
        case e: Throwable => {
          logger.error("Error getting input stream for file " + getFullPath, e)
          null
        }
      }
      finally {

      }

    inputStream
  }

  @throws(classOf[KamanjaException])
  def openForRead(): InputStream = {
    logger.debug(s"Opening SFTP file ($getFullPath) to read")

    try {
      /*manager = new StandardFileSystemManager()
      manager.init()*/

      val compressionType = CompressionUtil.getFileType(this, null)
      in = CompressionUtil.getProperInputStream(getDefaultInputStream, compressionType)
      in
    }
    catch{
      case e : Exception => throw new KamanjaException (e.getMessage, e)
      case e : Throwable => throw new KamanjaException (e.getMessage, e)
    }
  }

  @throws(classOf[KamanjaException])
  def read(buf : Array[Byte], length : Int) : Int = {
    read(buf, 0, length)
  }

  @throws(classOf[KamanjaException])
  def read(buf : Array[Byte], offset : Int, length : Int) : Int = {
    try {
      if (in == null) {
        logger.warn(s"Trying to read from SFTP file ($getFullPath) but input stream is null")
        return -1
      }
      logger.debug(s"Reading from SFTP file ($getFullPath)")
      in.read(buf, offset, length)
    }
    catch{
      case e : Exception => throw new KamanjaException (e.getMessage, e)
      case e : Throwable => throw new KamanjaException (e.getMessage, e)
    }
  }

  @throws(classOf[Exception])
  def moveTo(remoteNewFilePath : String) : Boolean = {

    if(getFullPath.equals(remoteNewFilePath)){
      logger.warn(s"Trying to move file ($getFullPath) but source and destination are the same")
      return false
    }

    val ui = new SftpUserInfo(connectionConfig.password, passphrase)
    logger.debug("Moving file {} to {}", getFullPath, remoteNewFilePath)
    try {
      session = getNewSession
      session.setUserInfo(ui)
      session.connect()
      val channel = session.openChannel("sftp")
      channel.connect()
      channelSftp = channel.asInstanceOf[ChannelSftp]


      if(!fileExists(channelSftp,getFullPath )) {
        logger.warn("Source file {} does not exists", getFullPath)
        return false
      }
      else

      //checking if dest file already exists
      if(fileExists(channelSftp, remoteNewFilePath)) {
        logger.info("File {} already exists. It will be deleted first", remoteNewFilePath)
        channelSftp.rm(remoteNewFilePath)
      }

      channelSftp.rename(getFullPath, remoteNewFilePath)

      return true
    }
    catch {
      case ex: Exception => {
        logger.error("Sftp File Handler - Error while trying to moving sftp file " +
          getFullPath + " to " + remoteNewFilePath, ex)
        return false
      }
      case ex: Throwable => {
        logger.error("Sftp File Handler - Error while trying to moving sftp file " +
          getFullPath + " to " + remoteNewFilePath, ex)
        return false
      }
    }
    finally{
      if(channelSftp != null) channelSftp.exit()
      if(session != null) session.disconnect()
    }

  }

  @throws(classOf[Exception])
  def delete() : Boolean = {
    val ui = new SftpUserInfo(connectionConfig.password, passphrase)

    try {
      session = getNewSession
      session.setUserInfo(ui)
      session.connect()
      val channel = session.openChannel("sftp")
      channel.connect()
      channelSftp = channel.asInstanceOf[ChannelSftp]
      channelSftp.rm(getFullPath)

      channelSftp.exit()
      session.disconnect()

      return true
    }
    catch {
      case ex: Exception => {
        logger.error("Sftp File Handler - Error while trying to delete sftp file " + getFullPath, ex)
        return false
      }
      case ex: Throwable => {
        logger.error("Sftp File Handler - Error while trying to delete sftp file " + getFullPath, ex)
        return false
      }
    }
    finally{
      if(channelSftp != null) channelSftp.exit()
      if(session != null) session.disconnect()
    }
  }

  @throws(classOf[Exception])
  def close(): Unit = {

    /*if(bufferedReader != null)
      bufferedReader.close()*/
    if(in != null) {
      try {
        in.close()
      }
      catch{
        case ex : Exception => logger.warn("Error while closing sftp file " + getFullPath, ex)
        case ex : Throwable => logger.warn("Error while closing sftp file " + getFullPath, ex)
      }
      in = null
    }

    if(channelSftp != null) channelSftp.exit()
    if(session != null) session.disconnect()
  }

  @throws(classOf[Exception])
  def length : Long = getRemoteFileAttrs.getSize

  @throws(classOf[Exception])
  def lastModified : Long = getRemoteFileAttrs.getMTime

  @throws(classOf[Exception])
  def exists(): Boolean = {
    val att = getRemoteFileAttrs
    att != null
  }

  private def fileExists(channel : ChannelSftp, file : String) : Boolean = {
      try{
        channelSftp.lstat(file)
        return true
      }
      catch{//source file does not exist, nothing to do
        case ee  : Exception => {
          //no need to log, file does not exist, calling threads will report
          return false
        }
        case ee  : Throwable => {
          return false
        }
      }
  }

  @throws(classOf[Exception])
  override def isFile: Boolean = !getRemoteFileAttrs.isDir

  @throws(classOf[Exception])
  override def isDirectory: Boolean = getRemoteFileAttrs.isDir

  private def getRemoteFileAttrs :  SftpATTRS = {
    try {
      val ui = new SftpUserInfo(connectionConfig.password, passphrase)
      session = getNewSession
      session.setUserInfo(ui);
      session.connect();
      val channel = session.openChannel("sftp");
      channel.connect();
      channelSftp = channel.asInstanceOf[ChannelSftp];
      channelSftp.lstat(getFullPath)
    }
    catch {
      case ex : Exception => {
        logger.error("Error while getting file attrs for file " + getFullPath, ex)
        null
      }
      case ex : Throwable => {
        logger.error("Error while getting file attrs for file " + getFullPath, ex)
        null
      }

    } finally {
      if(channelSftp != null) channelSftp.exit()
      if(session != null) session.disconnect()
    }
  }

  //no accurate way to make sure a file/folder is readable or writable by current user
  //api can only tell the unix rep. of file permissions but cannot find user name or group name of that file
  //so for now return true if exists
  override def isAccessible : Boolean = exists()
}

class SftpChangesMonitor (adapterName : String, modifiedFileCallback:(SmartFileHandler) => Unit) extends SmartFileMonitor{

  private var isMonitoring = false
  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)

  private var connectionConf : FileAdapterConnectionConfig = null
  private var monitoringConf :  FileAdapterMonitoringConfig = null

  private var monitorsExecutorService: ExecutorService = null

  private var host : String = _
  private var port : Int = _

  def init(adapterSpecificCfgJson: String): Unit ={
    val(_type, c, m) =  SmartFileAdapterConfiguration.parseSmartFileAdapterSpecificConfig(adapterName, adapterSpecificCfgJson)
    connectionConf = c
    monitoringConf = m

    if(connectionConf.hostsList == null || connectionConf.hostsList.length == 0){
      val err = "Invalid host for Smart SFTP File Adapter Config:" + adapterName
      throw new KamanjaException(err, null)
    }

    val hostTokens = connectionConf.hostsList(0).split(":")
    host = hostTokens(0)
    port = if(hostTokens(1) != null && hostTokens(1).length >0 ) hostTokens(1).toInt else 22 //default
  }

  def monitor: Unit ={

    val manager : StandardFileSystemManager  = new StandardFileSystemManager()

    isMonitoring = true
    //Initializes the file manager
    manager.init();

    //Setup our SFTP configuration
    val opts = createDefaultOptions(connectionConf)

    monitorsExecutorService = Executors.newFixedThreadPool(monitoringConf.locations.length)

    monitoringConf.locations.foreach(folderToWatch => {
      val dirMonitorthread = new Runnable() {
        private var targetRemoteFolder: String = _
        def init(dir: String) = targetRemoteFolder = dir

        override def run() = {
          try {

            val sftpEncodedUri = createConnectionString(connectionConf, targetRemoteFolder)

            val filesStatusMap = Map[String, SftpFileEntry]()
            var firstCheck = true

            while (isMonitoring) {

              try {
                logger.info(s"Checking configured SFTP directory ($targetRemoteFolder)...")

                val modifiedDirs = new ArrayBuffer[String]()
                modifiedDirs += sftpEncodedUri
                while (modifiedDirs.nonEmpty) {
                  //each time checking only updated folders: first find direct children of target folder that were modified
                  // then for each folder of these search for modified files and folders, repeat for the modified folders

                  val aFolder = modifiedDirs.head
                  val modifiedFiles = Map[SmartFileHandler, FileChangeType]() // these are the modified files found in folder $aFolder

                  modifiedDirs.remove(0)
                  findDirModifiedDirectChilds(aFolder, manager, filesStatusMap, modifiedDirs, modifiedFiles, firstCheck)
                  logger.debug("modifiedFiles map is {}", modifiedFiles)

                  if (modifiedFiles.nonEmpty)
                    modifiedFiles.foreach(tuple => {

                      /*val handler = new MofifiedFileCallbackHandler(tuple._1, tuple._2, modifiedFileCallback)
                   // run the callback in a different thread
                  //new Thread(handler).start()
                  globalFileMonitorCallbackService.execute(handler)*/
                      logger.debug("calling sftp monitor is calling file callback for Monitorcontroller for file {}", tuple._1.getFullPath)
                      modifiedFileCallback(tuple._1)

                    }
                    )
                }

              }
              catch {
                case ex: Exception => logger.error("Smart File Consumer (sftp Monitor) - Error while checking folder " + targetRemoteFolder, ex)
                case ex: Throwable => logger.error("Smart File Consumer (sftp Monitor) - Error while checking folder " + targetRemoteFolder, ex)
              }

              firstCheck = false

              logger.info(s"Sleepng for ${monitoringConf.waitingTimeMS} milliseconds...............................")
              Thread.sleep(monitoringConf.waitingTimeMS)
            }

            //if(!isMonitoring)
            //globalFileMonitorCallbackService.shutdown()
          }
          catch {
            case ex: Exception => {
              logger.error("Error while monitoring folder " + targetRemoteFolder, ex)
            }
            case ex: Throwable => {
              logger.error("Error while monitoring folder " + targetRemoteFolder, ex)
            }
          }
          finally {
            manager.close()
          }
        }
      }
      dirMonitorthread.init(folderToWatch)
      monitorsExecutorService.execute(dirMonitorthread)
    })

  }

  def shutdown: Unit ={
    isMonitoring = false
    monitorsExecutorService.shutdown()
  }


  private def findDirModifiedDirectChilds(parentfolder : String, manager : StandardFileSystemManager, filesStatusMap : Map[String, SftpFileEntry],
                                          modifiedDirs : ArrayBuffer[String], modifiedFiles : Map[SmartFileHandler, FileChangeType], isFirstCheck : Boolean){
    val parentfolderHashed = hashPath(parentfolder)//used for logging since path contains user and password
    logger.info("checking folder with full path: " + parentfolderHashed)

    val directChildren = getRemoteFolderContents(parentfolder, manager).sortWith(_.getContent.getLastModifiedTime < _.getContent.getLastModifiedTime)
    var changeType : FileChangeType = null //new, modified

    //logger.debug("got the following children for checked folder " + directChildren.map(c => c.getURL.toString).mkString(", "))
    //process each file reported by FS cache.
    directChildren.foreach(child => {
      val currentChildEntry = makeFileEntry(child)
      var isChanged = false
      val uniquePath = child.getURL().toString()

      if(!filesStatusMap.contains(uniquePath)){
        //path is new
        isChanged = true
        changeType = if(isFirstCheck) AlreadyExisting else New

        //logger.debug("file {} is {}", uniquePath, changeType.toString)

        filesStatusMap.put(uniquePath, currentChildEntry)
        if(currentChildEntry.isDirectory)
          modifiedDirs += uniquePath
      }
      else{
        //logger.debug("file {} is already in monitors filesStatusMap", uniquePath)

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
          //logger.debug("file {} is directory", uniquePath)
        }
        else{
          if(changeType == New || changeType == AlreadyExisting) {
            //logger.debug("file {} will be added to modifiedFiles map", uniquePath)
            val fileHandler = new SftpFileHandler(getPathOnly(uniquePath), connectionConf)
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
      //logger.debug("checking if file {} is deleted, parent is {}. comparing to folder {}",
        //fileEntry.name, fileEntry.parent, parentfolder)
      if(isDirectParentDir(fileEntry, parentfolder)){
        if(!directChildren.exists(fileStatus => fileStatus.getURL().toString.equals(fileEntry.name))) {
          //key that is no more in the folder => file/folder deleted
          logger.debug("file {} is no more under folder  {}, will be deleted from map", fileEntry.name, fileEntry.parent)
          deletedFiles += fileEntry.name
        }
        else {
          //logger.debug("file {} is still under folder  {}", fileEntry.name, fileEntry.parent)
        }
      }
    })

    //logger.debug("files to be deleted from map are : ", deletedFiles)
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
    //logger.debug("comparing folders {} and {}", getPathOnly(fileObj.parent), getPathOnly(dirUrl))
    getPathOnly(fileObj.parent).equals(getPathOnly(dirUrl))
  }

  //retrieve only path and remove connection ino
  private def getPathOnly(url : String) : String = {
    val hostIndex = url.indexOf(host)
    if(hostIndex < 0 )
      return url

    val afterHostUrl = url.substring(hostIndex + host.length)
    afterHostUrl.substring(afterHostUrl.indexOf("/"))
  }

}