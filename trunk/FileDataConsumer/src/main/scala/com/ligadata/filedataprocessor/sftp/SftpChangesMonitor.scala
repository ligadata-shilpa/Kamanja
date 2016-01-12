package com.ligadata.filedataprocessor.sftp

/**
 * Created by Yasser on 12/21/2015.
 */

import java.io.File
import java.io.FileInputStream
import java.net.URI
import java.net.URISyntaxException
import scala.collection.mutable.{ArrayBuffer, Map}
import org.apache.commons.vfs2.FileObject
import org.apache.commons.vfs2.FileSystemOptions
import org.apache.commons.vfs2.Selectors
import org.apache.commons.vfs2.impl.StandardFileSystemManager
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder
import com.ligadata.filedataprocessor.FileChangeType._
import com.ligadata.filedataprocessor.FileHandler
import java.io.IOException
import org.apache.commons.lang.NotImplementedException
import java.io.InputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import java.net.URLEncoder
import org.apache.logging.log4j.{ Logger, LogManager }


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

class SftpFileHandler extends FileHandler{
  private var remoteFullPath = ""
  def fullPath = remoteFullPath
    
  private var sftpConnectionConfig : SftpConnectionConfig = null
  private var manager : StandardFileSystemManager = null
  private var in : InputStream = null
  //private var bufferedReader : BufferedReader = null
  
  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)
  
  def this(path : String, config : SftpConnectionConfig){
    this()
    this.remoteFullPath = path
    sftpConnectionConfig = config
  }
  
   @throws(classOf[IOException])
  def openForRead(): Unit = {
    logger.info(s"Opening SFTP file ($fullPath) to read")
    val opts = createDefaultOptions
	   
    val sftpEncodedUri = "sftp://" + sftpConnectionConfig.userId + ":" + URLEncoder.encode(sftpConnectionConfig.password) +  
	       "@" + sftpConnectionConfig.serverAddress + "/" +  fullPath
	manager  = new StandardFileSystemManager()
    manager.init()
    
    val remoteFileObj = manager.resolveFile(sftpEncodedUri, opts)
    in = remoteFileObj.getContent().getInputStream()
    //bufferedReader = new BufferedReader(new InputStreamReader(in))
  }

  @throws(classOf[IOException])
  def read(buf : Array[Byte], length : Int) : Int = { 
	if (in == null){
	  logger.warn(s"Trying to read from SFTP file ($fullPath) but input stream is null")
      return -1
	}
	logger.info(s"Reading from SFTP file ($fullPath)")
    in.read(buf, 0, length)
  }

  @throws(classOf[IOException])
  def moveTo(remoteNewFilePath : String) : Boolean = {
    if(fullPath.equals(remoteNewFilePath)){
      logger.warn(s"Trying to move file ($fullPath) but source and destination are the same")
      return false
    }
     try {
       manager  = new StandardFileSystemManager()
       manager.init()
    
        val remoteSrcFile = manager.resolveFile(createConnectionString(sftpConnectionConfig, fullPath), createDefaultOptions())
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
       case ex : Exception => ex.printStackTrace()
        return false
     } finally {
       if(manager!=null)
    	   manager.close()
     }
  }
  
  @throws(classOf[IOException])
  def delete() : Boolean = {
     logger.info(s"Deleting file ($fullPath)")
     try {
       manager  = new StandardFileSystemManager()
       manager.init()
    
        val remoteFile = manager.resolveFile(createConnectionString(sftpConnectionConfig, fullPath), createDefaultOptions())
        remoteFile.delete()
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
       if(manager!=null)
    	   manager.close()
     }
  }

  @throws(classOf[IOException])
  def close(): Unit = {
    logger.info(s"closing SFTP file ($fullPath)")
    /*if(bufferedReader != null)
      bufferedReader.close()*/
    if(in != null)
      in.close()
    if(manager != null)
      manager.close()
  }
}

class SftpChangesMonitor (val sftpConnectionConfig : SftpConnectionConfig, val waitingTimeMS : Int,
                          modifiedFileCallback:(FileHandler, FileChangeType) => Unit){
  
  private var isMonitoring = false
  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)
  
  def monitorDirChanges(targetRemoteFolder : String, changeTypesToMonitor : Array[FileChangeType]){

    if(sftpConnectionConfig == null)
      throw new Exception("Invalid config params")

    val manager : StandardFileSystemManager  = new StandardFileSystemManager()
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
	          val modifiedFiles = Map[FileHandler, FileChangeType]() // these are the modified files found in folder $aFolder
	
	          modifiedDirs.remove(0)
	          findDirModifiedDirectChilds(aFolder, manager,  filesStatusMap, modifiedDirs, modifiedFiles, firstCheck)
	
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
	        }
	
	      }
	      catch{
	        case ex: Exception => ex.printStackTrace()
	      }
	
	      firstCheck = false
	
	      logger.info(s"Sleepng for $waitingTimeMS milliseconds...............................")
	      Thread.sleep(waitingTimeMS)
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
  }

  private def findDirModifiedDirectChilds(parentfolder : String, manager : StandardFileSystemManager, filesStatusMap : Map[String, SftpFileEntry],
                                          modifiedDirs : ArrayBuffer[String], modifiedFiles : Map[FileHandler, FileChangeType], isFirstCheck : Boolean){
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

          val fileHandler = new SftpFileHandler(getPathOnly(uniquePath), sftpConnectionConfig)
          modifiedFiles.put(fileHandler, changeType)
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