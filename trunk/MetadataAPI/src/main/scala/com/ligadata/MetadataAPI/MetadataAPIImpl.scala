/*
 * Copyright 2015 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ligadata.MetadataAPI

import java.util.Properties
import java.io._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.text.ParseException
import com.ligadata.MetadataAPI.MetadataAPI.ModelType
import com.ligadata.MetadataAPI.MetadataAPI.ModelType.ModelType

import scala.Enumeration
import scala.io._
import scala.collection.mutable.ArrayBuffer

import scala.collection.mutable._
import scala.reflect.runtime.{ universe => ru }

import com.ligadata.kamanja.metadata.ObjType._
import com.ligadata.kamanja.metadata._
import com.ligadata.kamanja.metadata.MdMgr._

import com.ligadata.kamanja.metadataload.MetadataLoad

// import com.ligadata.keyvaluestore._
import com.ligadata.HeartBeat.{MonitoringContext, HeartBeatUtil}
import com.ligadata.StorageBase.{ DataStore, Transaction }
import com.ligadata.KvBase.{ Key, Value, TimeRange }

import scala.util.parsing.json.JSON
import scala.util.parsing.json.{ JSONObject, JSONArray }
import scala.collection.immutable.Map
import scala.collection.immutable.HashMap
import scala.collection.mutable.HashMap

import com.google.common.base.Throwables

import com.ligadata.messagedef._
import com.ligadata.Exceptions._

import scala.xml.XML
import org.apache.logging.log4j._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import com.ligadata.ZooKeeper._
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import com.ligadata.keyvaluestore._
import com.ligadata.Serialize._
import com.ligadata.Utils._
import scala.util.control.Breaks._
import com.ligadata.AuditAdapterInfo._
import com.ligadata.SecurityAdapterInfo.SecurityAdapter
import com.ligadata.keyvaluestore.KeyValueManager
import com.ligadata.Exceptions.StackTrace

import java.util.Date

import org.json4s.jackson.Serialization

case class ParameterMap(RootDir: String, GitRootDir: String, MetadataStoreType: String, MetadataSchemaName: Option[String], /* MetadataAdapterSpecificConfig: Option[String], */ MetadataLocation: String, JarTargetDir: String, ScalaHome: String, JavaHome: String, ManifestPath: String, ClassPath: String, NotifyEngine: String, ZnodePath: String, ZooKeeperConnectString: String, MODEL_FILES_DIR: Option[String], TYPE_FILES_DIR: Option[String], FUNCTION_FILES_DIR: Option[String], CONCEPT_FILES_DIR: Option[String], MESSAGE_FILES_DIR: Option[String], CONTAINER_FILES_DIR: Option[String], COMPILER_WORK_DIR: Option[String], MODEL_EXEC_FLAG: Option[String], OUTPUTMESSAGE_FILES_DIR: Option[String])

case class Argument(ArgName: String, ArgTypeNameSpace: String, ArgTypeName: String)

// case class Attr(NameSpace: String, Name: String, Version: Long, Type: TypeDef)

// case class MessageStruct(NameSpace: String, Name: String, FullName: String, Version: Long, JarName: String, PhysicalName: String, DependencyJars: List[String], Attributes: List[Attr])
case class MessageDefinition(Message: MessageStruct)
case class ContainerDefinition(Container: MessageStruct)

case class ModelInfo(NameSpace: String, Name: String, Version: String, ModelType: String, JarName: String, PhysicalName: String, DependencyJars: List[String], InputAttributes: List[Attr], OutputAttributes: List[Attr])
case class ModelDefinition(Model: ModelInfo)

case class ZooKeeperInfo(ZooKeeperNodeBasePath: String, ZooKeeperConnectString: String, ZooKeeperSessionTimeoutMs: Option[String], ZooKeeperConnectionTimeoutMs: Option[String])

case class MetadataAPIConfig(APIConfigParameters: ParameterMap)

case class APIResultInfo(statusCode: Int, functionName: String, resultData: String, description: String)
case class APIResultJsonProxy(APIResults: APIResultInfo)

object MetadataAPIGlobalLogger {
    val loggerName = this.getClass.getName
    val logger = LogManager.getLogger(loggerName)
}

trait LogTrait {
    val logger = MetadataAPIGlobalLogger.logger
}

// The implementation class
object MetadataAPIImpl extends MetadataAPI with LogTrait {

  lazy val sysNS = "System" // system name space
  
  lazy val serializerType = "kryo"
  lazy val serializer = SerializerManager.GetSerializer(serializerType)
  lazy val metadataAPIConfig = new Properties()
  var zkc: CuratorFramework = null
  private var authObj: SecurityAdapter = null
  private var auditObj: AuditAdapter = null
  val configFile = System.getenv("HOME") + "/MetadataAPIConfig.json"
  var propertiesAlreadyLoaded = false
  var isInitilized: Boolean = false
  private var zkListener: ZooKeeperListener = _
  private var cacheOfOwnChanges: scala.collection.mutable.Set[String] = scala.collection.mutable.Set[String]()
  private var currentTranLevel: Long = _
  private var passwd: String = null
  private var compileCfg: String = ""
  private var heartBeat: HeartBeatUtil = null
  var zkHeartBeatNodePath = ""
  private val storageDefaultTime = 0L
  private val storageDefaultTxnId = 0L

  def getCurrentTranLevel = currentTranLevel

  var isCassandra = false
  private[this] val lock = new Object
  var startup = false

  private var tableStoreMap: Map[String, (String, DataStore)] = Map()

    /**
     * CloseZKSession
     */
  def CloseZKSession: Unit = lock.synchronized {
    if (zkc != null) {
      logger.debug("Closing zookeeper session ..")
      try {
        zkc.close()
        logger.debug("Closed zookeeper session ..")
        zkc = null
      } catch {
        case e: Exception => {
          logger.error("Unexpected Error while closing zookeeper session", e)
        }
      }
    }
  }

  /**
   *  getHealthCheck - will return all the health-check information for the nodeId specified.
   *  @param nodeId a cluster node: String - if no parameter specified, return health-check for all nodes
   *  @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
   *               method. If Security and/or Audit are configured, this value must be a value other than None.
   */
  def getHealthCheck(nodeId: String = "", userid: Option[String] = None): String = {
    try {
      val ids = parse(nodeId).values.asInstanceOf[List[String]]
      var apiResult = new ApiResultComplex(ErrorCodeConstants.Success, "GetHeartbeat", MonitorAPIImpl.getHeartbeatInfo(ids), ErrorCodeConstants.GetHeartbeat_Success)
      apiResult.toString
    } catch {
      case cce: java.lang.ClassCastException => {
        logger.warn("Failure processing GET_HEALTH_CHECK - cannot parse the list of desired nodes.", cce)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetHealthCheck", "No data available", ErrorCodeConstants.GetHeartbeat_Failed + " Error:Parsing Error")
        return apiResult.toString
      }
      case e: Exception => {
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetHealthCheck", "No data available", ErrorCodeConstants.GetHeartbeat_Failed + " Error: Unknown - see Kamanja Logs")
        logger.error("Failure processing GET_HEALTH_CHECK - unknown", e)
        return apiResult.toString
      }
    }

  }

  /**
   * InitSecImpl  - 1. Create the Security Adapter class.  The class name and jar name containing
   *                the implementation of that class are specified in the CONFIG FILE.
   *                2. Create the Audit Adapter class, The class name and jar name containing
   *                the implementation of that class are specified in the CONFIG FILE.
   */
  def InitSecImpl: Unit = {
    logger.debug("Establishing connection to domain security server..")
    val classLoader = new KamanjaLoaderInfo

    // Validate the Auth/Audit flags for valid input.
    if ((metadataAPIConfig.getProperty("DO_AUTH") != null) &&
      (!metadataAPIConfig.getProperty("DO_AUTH").equalsIgnoreCase("YES") &&
        !metadataAPIConfig.getProperty("DO_AUTH").equalsIgnoreCase("NO"))) {
      throw new Exception("Invalid value for DO_AUTH detected.  Correct it and restart")
    }
    if ((metadataAPIConfig.getProperty("DO_AUDIT") != null) &&
      (!metadataAPIConfig.getProperty("DO_AUDIT").equalsIgnoreCase("YES") &&
        !metadataAPIConfig.getProperty("DO_AUDIT").equalsIgnoreCase("NO"))) {
      throw new Exception("Invalid value for DO_AUDIT detected.  Correct it and restart")
    }

    // If already have one, use that!
    if (authObj == null && (metadataAPIConfig.getProperty("DO_AUTH") != null) && (metadataAPIConfig.getProperty("DO_AUTH").equalsIgnoreCase("YES"))) {
      createAuthObj(classLoader)
    }
    if (auditObj == null && (metadataAPIConfig.getProperty("DO_AUDIT") != null) && (metadataAPIConfig.getProperty("DO_AUDIT").equalsIgnoreCase("YES"))) {
      createAuditObj(classLoader)
    }
  }

    /**
     * createAuthObj - private method to instantiate an authObj
     * @param classLoader a
     */
  private def createAuthObj(classLoader: KamanjaLoaderInfo): Unit = {
    // Load the location and name of the implementing class from the
    val implJarName = if (metadataAPIConfig.getProperty("SECURITY_IMPL_JAR") == null) "" else metadataAPIConfig.getProperty("SECURITY_IMPL_JAR").trim
    val implClassName = if (metadataAPIConfig.getProperty("SECURITY_IMPL_CLASS") == null) "" else metadataAPIConfig.getProperty("SECURITY_IMPL_CLASS").trim
    logger.debug("Using " + implClassName + ", from the " + implJarName + " jar file")
    if (implClassName == null) {
      logger.error("Security Adapter Class is not specified")
      return
    }

    // Add the Jarfile to the class loader
    loadJar(classLoader, implJarName)

    try {
      Class.forName(implClassName, true, classLoader.loader)
    } catch {
      case e: Exception => {
        logger.error("Failed to load Security Adapter class %s".format(implClassName), e)
        throw e // Rethrow
      }
    }

    // All is good, create the new class
    var className = Class.forName(implClassName, true, classLoader.loader).asInstanceOf[Class[SecurityAdapter]]
    authObj = className.newInstance
    authObj.init
    logger.debug("Created class " + className.getName)
  }

    /**
     * createAuditObj - private method to instantiate an authObj
     * @param classLoader a
     */
  private def createAuditObj(classLoader: KamanjaLoaderInfo): Unit = {
    // Load the location and name of the implementing class froms the
    val implJarName = if (metadataAPIConfig.getProperty("AUDIT_IMPL_JAR") == null) "" else metadataAPIConfig.getProperty("AUDIT_IMPL_JAR").trim
    val implClassName = if (metadataAPIConfig.getProperty("AUDIT_IMPL_CLASS") == null) "" else metadataAPIConfig.getProperty("AUDIT_IMPL_CLASS").trim
    logger.debug("Using " + implClassName + ", from the " + implJarName + " jar file")
    if (implClassName == null) {
      logger.error("Audit Adapter Class is not specified")
      return
    }

    // Add the Jarfile to the class loader
    loadJar(classLoader, implJarName)

        try
            Class.forName(implClassName, true, classLoader.loader)
        catch {
            case e: Exception => {
                logger.error("Failed to load Audit Adapter class %s".format(implClassName), e)
                throw e // Rethrow
            }
        }
    // All is good, create the new class
    var className = Class.forName(implClassName, true, classLoader.loader).asInstanceOf[Class[AuditAdapter]]
    auditObj = className.newInstance
    auditObj.init(metadataAPIConfig.getProperty("AUDIT_PARMS"))
    logger.debug("Created class " + className.getName)
  }

    /**
     * loadJar- load the specified jar into the classLoader
     * @param classLoader a
     * @param implJarName a
     */
  private def loadJar(classLoader: KamanjaLoaderInfo, implJarName: String): Unit = {
    // Add the Jarfile to the class loader
    val tmpJarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS")
    val jarPaths = if (tmpJarPaths != null) tmpJarPaths.split(",").toSet else scala.collection.immutable.Set[String]()
    val jarName = com.ligadata.Utils.Utils.GetValidJarFile(jarPaths, implJarName)
    val fl = new File(jarName)
    if (fl.exists) {
      try {
        classLoader.loader.addURL(fl.toURI().toURL())
        logger.debug("Jar " + implJarName.trim + " added to class path.")
        classLoader.loadedJars += fl.getPath()
      } catch {
        case e: Exception => {
          logger.error("Failed to add " + implJarName + " due to internal exception ", e)
          return
        }
      }
    } else {
      logger.error("Unable to locate Jar '" + implJarName + "'")
      return
    }
  }

    /**
     * checkAuth
     * @param usrid a
     * @param password a
     * @param role a
     * @param privilige a
     * @return <description please>
     */
  def checkAuth(usrid: Option[String], password: Option[String], role: Option[String], privilige: String): Boolean = {

    var authParms: java.util.Properties = new Properties
    // Do we want to run AUTH?
    if ((metadataAPIConfig.getProperty("DO_AUTH") == null) ||
      (metadataAPIConfig.getProperty("DO_AUTH") != null && !metadataAPIConfig.getProperty("DO_AUTH").equalsIgnoreCase("YES"))) {
      return true
    }

    // check if the Auth object exists
    if (authObj == null) return false

    // Run the Auth - if userId is supplied, defer to that.
    if ((usrid == None) && (role == None)) return false

    if (usrid != None) authParms.setProperty("userid", usrid.get.asInstanceOf[String])
    if (password != None) authParms.setProperty("password", password.get.asInstanceOf[String])
    if (role != None) authParms.setProperty("role", role.get.asInstanceOf[String])
    authParms.setProperty("privilige", privilige)

    return authObj.performAuth(authParms)
  }

    /**
     * getPrivilegeName
     * @param op <description please>
     * @param objName <description please>
     * @return <description please>
     */
  def getPrivilegeName(op: String, objName: String): String = {
    // check if the Auth object exists
    logger.debug("op => " + op)
    logger.debug("objName => " + objName)
    if (authObj == null) return ""
    return authObj.getPrivilegeName(op, objName)
  }

  /**
   * getSSLCertificatePath
   */
  def getSSLCertificatePath: String = {
    val certPath = metadataAPIConfig.getProperty("SSL_CERTIFICATE")
    if (certPath != null) return certPath
    ""
  }

  /**
   * getSSLCertificatePasswd
   */
  def getSSLCertificatePasswd: String = {
    if (passwd != null) return passwd
    ""
  }

    /**
     * setSSLCertificatePasswd
     * @param pw <description please>
     */
  def setSSLCertificatePasswd(pw: String) = {
    passwd = pw
  }

  /**
   * getCurrentTime - Return string representation of the current Date/Time
   */
  def getCurrentTime: String = {
    new Date().getTime().toString()
  }

    /**
     * logAuditRec - Record an Audit event using the audit adapter
     * @param userOrRole the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. The default is None, but if Security and/or Audit are configured, this value is of little practical use.
     *               Supply one.
     * @param userPrivilege <description please>
     * @param action <description please>
     * @param objectText <description please>
     * @param success <description please>
     * @param transactionId <description please>
     * @param objName <description please>
     */
  def logAuditRec(userOrRole: Option[String], userPrivilege: Option[String], action: String, objectText: String, success: String, transactionId: String, objName: String) = {
    if (auditObj != null) {
      val aRec = new AuditRecord

      // If no userName is provided here, that means that somehow we are not running with Security but with Audit ON.
      var userName = "undefined"
      if (userOrRole != None) {
        userName = userOrRole.get
      }

      // If no priv is provided here, that means that somehow we are not running with Security but with Audit ON.
      var priv = "undefined"
      if (userPrivilege != None) {
        priv = userPrivilege.get
      }

      aRec.userOrRole = userName
      aRec.userPrivilege = priv
      aRec.actionTime = getCurrentTime
      aRec.action = action
      aRec.objectAccessed = objName
      aRec.success = success
      aRec.transactionId = transactionId
      aRec.notes = objectText
      try {
        auditObj.addAuditRecord(aRec)
      } catch {
        case e: Exception => {
          logger.error("", e)
          throw UpdateStoreFailedException("Failed to save audit record" + aRec.toString + ":" + e.getMessage(), e)
        }
      }
    }
  }

    /**
     * Get an audit record from the audit adapter.
     * @param startTime <description please>
     * @param endTime <description please>
      * @param userOrRole the identity to be used by the security adapter to ascertain if this user has access permissions for this
      *               method. If Security and/or Audit are configured, this value should be supplied.
     * @param action <description please>
     * @param objectAccessed <description please>
     * @return <description please>
     */
  def getAuditRec(startTime: Date, endTime: Date, userOrRole: String, action: String, objectAccessed: String): String = {
    var apiResultStr = ""
    if (auditObj == null) {
      apiResultStr = "no audit records found "
      return apiResultStr
    }
    try {
      val recs = auditObj.getAuditRecord(startTime, endTime, userOrRole, action, objectAccessed)
      if (recs.length > 0) {
        apiResultStr = JsonSerializer.SerializeAuditRecordsToJson(recs)
      } else {
        apiResultStr = "no audit records found "
      }
    } catch {
      case e: Exception => {
        logger.error("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "Failed to fetch all the audit objects:", null, "Error :" + e.toString)
        apiResultStr = apiResult.toString()
      }
    }
    logger.debug(apiResultStr)
    apiResultStr
  }

    /**
     * parseDateStr
     * @param dateStr <description please>
     * @return <description please>
     */
  def parseDateStr(dateStr: String): Date = {
    try {
      val format = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
      val d = format.parse(dateStr)
      d
    } catch {
      case e: ParseException => {
        logger.debug("", e)
        val format = new java.text.SimpleDateFormat("yyyyMMdd")
        val d = format.parse(dateStr)
        d
      }
    }
  }

    /**
     * getLeaderHost
     * @param leaderNode <description please>
     * @return <description please>
     */
  def getLeaderHost(leaderNode: String): String = {
    val nodes = MdMgr.GetMdMgr.Nodes.values.toArray
    if (nodes.length == 0) {
      logger.debug("No Nodes found ")
      val apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetLeaderHost", null, ErrorCodeConstants.Get_Leader_Host_Failed_Not_Available + " :" + leaderNode)
      apiResult.toString()
    } else {
      val nhosts = nodes.filter(n => n.nodeId == leaderNode)
      if (nhosts.length == 0) {
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetLeaderHost", null, ErrorCodeConstants.Get_Leader_Host_Failed_Not_Available + " :" + leaderNode)
        apiResult.toString()
      } else {
        val nhost = nhosts(0)
        logger.debug("node addr => " + nhost.NodeAddr)
        nhost.NodeAddr
      }
    }
  }

    /**
     * getAuditRec
     * @param filterParameters <description please>
     * @return <description please>
     */
  def getAuditRec(filterParameters: Array[String]): String = {
    var apiResultStr = ""
    if (auditObj == null) {
      apiResultStr = "no audit records found "
      return apiResultStr
    }
    try {
      var audit_interval = 10
      var ai = metadataAPIConfig.getProperty("AUDIT_INTERVAL")
      if (ai != null) {
        audit_interval = ai.toInt
      }
      var startTime: Date = new Date((new Date).getTime() - audit_interval * 60000)
      var endTime: Date = new Date()
      var userOrRole: String = null
      var action: String = null
      var objectAccessed: String = null

      if (filterParameters != null) {
        val paramCnt = filterParameters.size
        paramCnt match {
          case 1 => {
            startTime = parseDateStr(filterParameters(0))
          }
          case 2 => {
            startTime = parseDateStr(filterParameters(0))
            endTime = parseDateStr(filterParameters(1))
          }
          case 3 => {
            startTime = parseDateStr(filterParameters(0))
            endTime = parseDateStr(filterParameters(1))
            userOrRole = filterParameters(2)
          }
          case 4 => {
            startTime = parseDateStr(filterParameters(0))
            endTime = parseDateStr(filterParameters(1))
            userOrRole = filterParameters(2)
            action = filterParameters(3)
          }
          case 5 => {
            startTime = parseDateStr(filterParameters(0))
            endTime = parseDateStr(filterParameters(1))
            userOrRole = filterParameters(2)
            action = filterParameters(3)
            objectAccessed = filterParameters(4)
          }
        }
      } else {
        logger.debug("filterParameters is null")
      }
      apiResultStr = getAuditRec(startTime, endTime, userOrRole, action, objectAccessed)
    } catch {
      case e: Exception => {
        logger.debug("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "Failed to fetch all the audit objects:", null, "Error :" + e.toString)
        apiResultStr = apiResult.toString()
      }
    }
    logger.debug(apiResultStr)
    apiResultStr
  }

  /**
   * InitZooKeeper - Establish a connection to zookeeper
   */ 
  def InitZooKeeper: Unit = {
    logger.debug("Connect to zookeeper..")
    if (zkc != null) {
      // Zookeeper is already connected
      return
    }

    val zkcConnectString = GetMetadataAPIConfig.getProperty("ZOOKEEPER_CONNECT_STRING")
    val znodePath = GetMetadataAPIConfig.getProperty("ZNODE_PATH") + "/metadataupdate"
    zkHeartBeatNodePath = metadataAPIConfig.getProperty("ZNODE_PATH") + "/monitor/metadata/" + metadataAPIConfig.getProperty("NODE_ID").toString
    logger.debug("Connect To ZooKeeper using " + zkcConnectString)
    try {
      CreateClient.CreateNodeIfNotExists(zkcConnectString, zkHeartBeatNodePath)
      CreateClient.CreateNodeIfNotExists(zkcConnectString, znodePath)
      zkc = CreateClient.createSimple(zkcConnectString)
    } catch {
      case e: Exception => {
        logger.debug("", e)
        throw InternalErrorException("Failed to start a zookeeper session with(" + zkcConnectString + ")", e)
      }
    }
  }

    /**
     * shutdownAuditAdapter
     */
  private def shutdownAuditAdapter(): Unit = {
    if (auditObj != null) auditObj.Shutdown
  }

    /**
     * GetMetadataAPIConfig
     * @return <description please>
     */
  def GetMetadataAPIConfig: Properties = {
    metadataAPIConfig
  }

  private var mainDS: DataStore = _

  def GetMainDS: DataStore = mainDS

  def GetObject(bucketKeyStr: String, typeName: String): Value = {
    val (containerName, store) = tableStoreMap(typeName)
    var objs = new Array[Value](1)
    val getObjFn = (k: Key, v: Value) => {
      objs(0) = v
    }

    try {
      objs(0) = null
      store.get(containerName, Array(TimeRange(storageDefaultTime, storageDefaultTime)), Array(Array(bucketKeyStr)), getObjFn)
      if (objs(0) == null)
        throw ObjectNotFoundException("Object %s not found in container %s".format(bucketKeyStr, containerName), null)
      objs(0)
    } catch {
      case e: ObjectNotFoundException => {
        logger.debug("ObjectNotFound Exception", e)
        throw e
      }
      case e: Exception => {
        logger.debug("General Exception", e)
        throw ObjectNotFoundException(e.getMessage(), e)
      }
    }
  }

  def SaveObject(bucketKeyStr: String, value: Array[Byte], typeName: String, serializerTyp: String) {


    val (containerName, store) = tableStoreMap(typeName)
    val k = Key(storageDefaultTime, Array(bucketKeyStr), storageDefaultTxnId, 0)
    val v = Value(serializerTyp, value)

    try {
      store.put(containerName, k, v)
    } catch {
      case e: Exception => {
        logger.error("Failed to insert/update object for : " + bucketKeyStr, e)
        throw UpdateStoreFailedException("Failed to insert/update object for : " + bucketKeyStr, e)
      }
    }
  }

    /**
     * SaveObjectList
     * @param keyList
     * @param valueList
     * @param typeName
     * @param serializerTyp
     */
  def SaveObjectList(keyList: Array[String], valueList: Array[Array[Byte]], typeName: String, serializerTyp: String) {
    val (containerName, store) = tableStoreMap(typeName)

    var i = 0
    /*
    keyList.foreach(key => {
      var value = valueList(i)
      logger.debug("Writing Key:" + key)
      SaveObject(key, value, store, containerName)
      i = i + 1
    })
*/
    var storeObjects = new Array[(Key, Value)](keyList.length)
    i = 0
    keyList.foreach(bucketKeyStr => {
      var value = valueList(i)
      val k = Key(storageDefaultTime, Array(bucketKeyStr), storageDefaultTxnId, 0)
      val v = Value(serializerTyp, value)
      storeObjects(i) = (k, v)
      i = i + 1
    })

    try {
      store.put(Array((containerName, storeObjects)))
    } catch {
      case e: Exception => {
        logger.error("Failed to insert/update objects for : " + keyList.mkString(","), e)
        throw UpdateStoreFailedException("Failed to insert/update object for : " + keyList.mkString(","), e)
      }
    }
  }

    /**
     * Remove all of the elements with the supplied keys in the list from the supplied DataStore
     * @param keyList
     * @param store
     */
  def RemoveObjectList(keyList: Array[String], typeName: String) {
    val (containerName, store) = tableStoreMap(typeName)
    var i = 0
    var delKeys = new Array[(Key)](keyList.length)
    i = 0
    keyList.foreach(bucketKeyStr => {
      val k = Key(storageDefaultTime, Array(bucketKeyStr), storageDefaultTxnId, 0)
      delKeys(i) = k
      i = i + 1
    })

    try {
      store.del(containerName, delKeys)
    } catch {
      case e: Exception => {
        logger.error("Failed to delete object batch for : " + keyList.mkString(","), e)
        throw UpdateStoreFailedException("Failed to delete object batch for : " + keyList.mkString(","), e)
      }
    }
  }

    /**
     * Answer which table the supplied BaseElemeDef is stored
     * @param obj
     * @return
     */
  def getMdElemTypeName(obj: BaseElemDef): String = {
    obj match {
      case o: ModelDef => {
        "models"
      }
      case o: MessageDef => {
        "messages"
      }
      case o: ContainerDef => {
        "containers"
      }
      case o: FunctionDef => {
        "functions"
      }
      case o: AttributeDef => {
        "concepts"
      }
      case o: OutputMsgDef => {
        "outputmsgs"
      }
      case o: BaseTypeDef => {
        "types"
      }
      case _ => {
        logger.error("getMdElemTypeName is not implemented for objects of type " + obj.getClass.getName)
        throw InternalErrorException("getMdElemTypeName is not implemented for objects of type " + obj.getClass.getName, null)
      }
    }
  }

    /**
     * getObjectType
     * @param obj <description please>
     * @return <description please>
     */
  def getObjectType(obj: BaseElemDef): String = {
    val className = obj.getClass().getName();
    className.split("\\.").last
  }

    /**
     * SaveObjectList
     *
     * The following batch function is useful when we store data in single table
     * If we use Storage component library, note that table itself is associated with a single
     * database connection( which itself can be mean different things depending on the type
     * of datastore, such as cassandra, hbase, etc..)
     *
     * @param objList
     * @param typeName
     */
  def SaveObjectList(objList: Array[BaseElemDef], typeName: String) {

    logger.debug("Save " + objList.length + " objects in a single transaction ")
    val tranId = GetNewTranId
    var keyList = new Array[String](objList.length)
    var valueList = new Array[Array[Byte]](objList.length)
    try {
      var i = 0;
      objList.foreach(obj => {
        obj.tranId = tranId
        val key = (getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
        var value = serializer.SerializeObjectToByteArray(obj)
        keyList(i) = key
        valueList(i) = value
        i = i + 1
      })
      SaveObjectList(keyList, valueList, typeName, serializerType)
    } catch {
      case e: Exception => {
        logger.error("Failed to insert/update object for : " + keyList.mkString(","), e)
        throw UpdateStoreFailedException("Failed to insert/update object for : " + keyList.mkString(","), e)
      }
    }
  }

    /**
     * SaveObjectList
     * The following batch function is useful when we store data in multiple tables
     * If we use Storage component library, note that each table is associated with a different
     * database connection( which itself can be mean different things depending on the type
     * of datastore, such as cassandra, hbase, etc..)
     *
     * @param objList
     */
  def SaveObjectList(objList: Array[BaseElemDef]) {
    logger.debug("Save " + objList.length + " objects in a single transaction ")
    val tranId = GetNewTranId
    var saveDataMap = scala.collection.mutable.Map[String, ArrayBuffer[(Key, Value)]]()

    try {
      var i = 0;
      objList.foreach(obj => {
        obj.tranId = tranId
        val key = (getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
        var value = serializer.SerializeObjectToByteArray(obj)
        val elemTyp = getMdElemTypeName(obj)

        val k = Key(storageDefaultTime, Array(key), storageDefaultTxnId, 0)
        val v = Value(serializerType, value)

        val ab = saveDataMap.getOrElse(elemTyp, null)
        if (ab != null) {
          ab += ((k, v))
          saveDataMap(elemTyp) = ab
        } else {
          val newab = ArrayBuffer[(Key, Value)]()
          newab += ((k, v))
          saveDataMap(elemTyp) = newab
        }
        i = i + 1
      })

      var storeData = scala.collection.mutable.Map[String, (DataStore, ArrayBuffer[(String, Array[(Key, Value)])])]()

      saveDataMap.foreach(elemTypData => {
        val storeInfo = tableStoreMap(elemTypData._1)
        val oneStoreData = storeData.getOrElse(storeInfo._1, null)
        if (oneStoreData != null) {
          oneStoreData._2 += ((elemTypData._1, elemTypData._2.toArray))
          storeData(storeInfo._1) = ((oneStoreData._1, oneStoreData._2))
        } else {
          val ab = ArrayBuffer[(String, Array[(Key, Value)])]()
          ab += ((elemTypData._1, elemTypData._2.toArray))
          storeData(storeInfo._1) = ((storeInfo._2, ab))
        }
      })

      storeData.foreach(oneStoreData => {
        try {
          oneStoreData._2._1.put(oneStoreData._2._2.toArray)
        } catch {
          case e: Exception => {
            logger.error("Failed to insert/update objects in : " + oneStoreData._1, e)
            throw UpdateStoreFailedException("Failed to insert/update object for : " + oneStoreData._1, e)
          }
        }
      })
    } catch {
      case e: Exception => {
        logger.error("Failed to insert/update objects", e)
        throw UpdateStoreFailedException("Failed to insert/update objects", e)
      }
    }
  }

    /**
     * SaveOutputMsObjectList
     * @param objList <description please>
     */
  def SaveOutputMsObjectList(objList: Array[BaseElemDef]) {
    SaveObjectList(objList, "outputmsgs")
  }

    /**
     * SaveObject (use default serializerType (i.e., currently kryo).
     * @param key
     * @param value
     * @param typeName
  def SaveObject(key: String, value: String, typeName: String) {
    val ba = serializer.SerializeObjectToByteArray(value)
    SaveObject(key, ba, store, containerName, serializerType)
  }
     */

    /**
     * UpdateObject
     * @param key
     * @param value
     * @param typeName
     * @param serializerTyp
     */
  def UpdateObject(key: String, value: Array[Byte], typeName: String, serializerTyp: String) {
     SaveObject(key, value, typeName, serializerTyp)
  }

    /**
     * ZooKeeperMessage
     * @param objList
     * @param operations
     * @return
     */
  def ZooKeeperMessage(objList: Array[BaseElemDef], operations: Array[String]): Array[Byte] = {
    try {
      val notification = JsonSerializer.zkSerializeObjectListToJson("Notifications", objList, operations)
      notification.getBytes
    } catch {
      case e: Exception => {
        logger.error("", e)
        throw InternalErrorException("Failed to generate a zookeeper message from the objList", e)
      }
    }
  }

   def UpdateTranId (objList:Array[BaseElemDef] ): Unit ={
    var max: Long = 0
     objList.foreach(obj =>{
       max = scala.math.max(max, obj.TranId)
     })
    if (currentTranLevel < max) currentTranLevel = max
    PutTranId(max)
  }


    /**
     * NotifyEngine
     * @param objList <description please>
     * @param operations <description please>
     */
  def NotifyEngine(objList: Array[BaseElemDef], operations: Array[String]) {
    try {
      val notifyEngine = GetMetadataAPIConfig.getProperty("NOTIFY_ENGINE")

      // We have to update the currentTranLevel here, since addObjectToCache method does not have the tranId in object
      // yet (a bug that is being ractified now)...  We can remove this code when that is fixed.
      var max: Long = 0
      objList.foreach(obj => {
        max = scala.math.max(max, obj.TranId)
      })
      if (currentTranLevel < max) currentTranLevel = max

      if (notifyEngine != "YES") {
        logger.warn("Not Notifying the engine about this operation because The property NOTIFY_ENGINE is not set to YES")
        PutTranId(max)
        return
      }

      // Set up the cache of CACHES!!!! Since we are listening for changes to Metadata, we will be notified by Zookeeper
      // of this change that we are making.  This cache of Caches will tell us to ignore this.
      var corrId: Int = 0
      objList.foreach(elem => {
        cacheOfOwnChanges.add((operations(corrId) + "." + elem.NameSpace + "." + elem.Name + "." + elem.Version).toLowerCase)
        corrId = corrId + 1
      })

      val data = ZooKeeperMessage(objList, operations)
      InitZooKeeper
      val znodePath = GetMetadataAPIConfig.getProperty("ZNODE_PATH") + "/metadataupdate"
      logger.debug("Set the data on the zookeeper node " + znodePath)
      zkc.setData().forPath(znodePath, data)

      PutTranId(max)
    } catch {
      case e: Exception => {
        logger.error("", e)
        throw InternalErrorException("Failed to notify a zookeeper message from the objectList", e)
      }
    }
  }

    /**
     * GetNewTranId
     * @return <description please>
     */
  def GetNewTranId: Long = {
    try {
      val obj = GetObject("transaction_id", "transaction_id")
      val idStr = new String(obj.serializedInfo)
      idStr.toLong + 1
    } catch {
      case e: ObjectNotFoundException => {
        // first time
        1
      }
      case e: Exception => {
        throw TranIdNotFoundException("Unable to retrieve the transaction id", e)
      }
    }
  }

    /**
     * GetTranId
     * @return <description please>
     */
  def GetTranId: Long = {
    try {
      val obj = GetObject("transaction_id", "transaction_id")
      val idStr = new String(obj.serializedInfo)
      idStr.toLong
    } catch {
      case e: ObjectNotFoundException => {
        // first time
        0
      }
      case e: Exception => {
        throw TranIdNotFoundException("Unable to retrieve the transaction id", e)
      }
    }
  }

    /**
     * PutTranId
     * @param tId <description please>
     */
  def PutTranId(tId: Long) = {
    try {
      SaveObject("transaction_id", tId.toString.getBytes, "transaction_id", "")
    } catch {
      case e: Exception => {
        logger.error("", e)
        throw UpdateStoreFailedException("Unable to Save the transaction id " + tId, e)
      }
    }
  }

    /**
     * SaveObject
     * @param obj <description please>
     * @param mdMgr the metadata manager receiver
     * @return <description please>
     */
  def SaveObject(obj: BaseElemDef, mdMgr: MdMgr): Boolean = {
    try {
      val key = (getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
      val dispkey = (getObjectType(obj) + "." + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version)).toLowerCase
      obj.tranId = GetNewTranId

      //val value = JsonSerializer.SerializeObjectToJson(obj)
      logger.debug("Serialize the object: name of the object => " + dispkey)
      var value = serializer.SerializeObjectToByteArray(obj)

      val saveObjFn = () => {
        SaveObject(key, value, getMdElemTypeName(obj), serializerType) // Make sure getMdElemTypeName is success full all types we handle here
      }

      obj match {
        case o: ModelDef => {
          logger.debug("Adding the model to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddModelDef(o, false)
        }
        case o: MessageDef => {
          logger.debug("Adding the message to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddMsg(o)
        }
        case o: ContainerDef => {
          logger.debug("Adding the container to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddContainer(o)
        }
        case o: FunctionDef => {
          val funcKey = (obj.getClass().getName().split("\\.").last + "." + o.typeString).toLowerCase
          logger.debug("Adding the function to the cache: name of the object =>  " + funcKey)
          saveObjFn()
          mdMgr.AddFunc(o)
        }
        case o: AttributeDef => {
          logger.debug("Adding the attribute to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddAttribute(o)
        }
        case o: ScalarTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddScalar(o)
        }
        case o: ArrayTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddArray(o)
        }
        case o: ArrayBufTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddArrayBuffer(o)
        }
        case o: ListTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddList(o)
        }
        case o: QueueTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddQueue(o)
        }
        case o: SetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddSet(o)
        }
        case o: TreeSetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddTreeSet(o)
        }
        case o: SortedSetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddSortedSet(o)
        }
        case o: MapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddMap(o)
        }
        case o: ImmutableMapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddImmutableMap(o)
        }
        case o: HashMapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddHashMap(o)
        }
        case o: TupleTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddTupleType(o)
        }
        case o: ContainerTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddContainerType(o)
        }
        case o: OutputMsgDef => {
          logger.trace("Adding the Output Message to the cache: name of the object =>  " + dispkey)
          saveObjFn()
          mdMgr.AddOutputMsg(o)
        }
        case _ => {
          logger.error("SaveObject is not implemented for objects of type " + obj.getClass.getName)
        }
      }
      true
    } catch {
      case e: AlreadyExistsException => {
        logger.error("Failed to Save the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + ")", e)
        false
      }
      case e: Exception => {
        logger.error("Failed to Save the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + ")", e)
        false
      }
    }
  }

    /**
     * UpdateObjectInDB
     * @param obj <description please>
     */
  def UpdateObjectInDB(obj: BaseElemDef) {
    try {
      val key = (getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
      val dispkey = (getObjectType(obj) + "." + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version)).toLowerCase

      logger.debug("Serialize the object: name of the object => " + dispkey)
      var value = serializer.SerializeObjectToByteArray(obj)

      val updObjFn = () => {
        UpdateObject(key, value, getMdElemTypeName(obj), serializerType) // Make sure getMdElemTypeName is success full all types we handle here
      }

      obj match {
        case o: ModelDef => {
          logger.debug("Updating the model in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: MessageDef => {
          logger.debug("Updating the message in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: ContainerDef => {
          logger.debug("Updating the container in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: FunctionDef => {
          val funcKey = (obj.getClass().getName().split("\\.").last + "." + o.typeString).toLowerCase
          logger.debug("Updating the function in the DB: name of the object =>  " + funcKey)
          updObjFn()
        }
        case o: AttributeDef => {
          logger.debug("Updating the attribute in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: ScalarTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: ArrayTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: ArrayBufTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: ListTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: QueueTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: SetTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: TreeSetTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: SortedSetTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: MapTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: ImmutableMapTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: HashMapTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: TupleTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: ContainerTypeDef => {
          logger.debug("Updating the Type in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case o: OutputMsgDef => {
          logger.debug("Updating the output message in the DB: name of the object =>  " + dispkey)
          updObjFn()
        }
        case _ => {
          logger.error("UpdateObject is not implemented for objects of type " + obj.getClass.getName)
        }
      }
    } catch {
      case e: AlreadyExistsException => {
        logger.error("Failed to Update the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + ")", e)
      }
      case e: Exception => {
        logger.error("Failed to Update the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + ")", e)
      }
    }
  }

    /**
     * GetJarAsArrayOfBytes
     * @param jarName <description please>
     * @return <description please>
     */
  def GetJarAsArrayOfBytes(jarName: String): Array[Byte] = {
    try {
      val iFile = new File(jarName)
      if (!iFile.exists) {
        logger.error("Jar file (" + jarName + ") is not found: ")
        throw new FileNotFoundException("Jar file (" + jarName + ") is not found: ")
      }
      val bis = new BufferedInputStream(new FileInputStream(iFile));
      val baos = new ByteArrayOutputStream();
      var readBuf = new Array[Byte](1024) // buffer size
      // read until a single byte is available
      while (bis.available() > 0) {
        val c = bis.read();
        baos.write(c)
      }
      bis.close();
      baos.toByteArray()
    } catch {
      case e: IOException => {
        logger.error("", e)
        throw new FileNotFoundException("Failed to Convert the Jar (" + jarName + ") to array of bytes. Message:" + e.getMessage())
      }
      case e: Exception => {
        logger.error("", e)
        throw InternalErrorException("Failed to Convert the Jar (" + jarName + ") to array of bytes", e)
      }
    }
  }

    /**
     * PutArrayOfBytesToJar
     * @param ba <description please>
     * @param jarName <description please>
     */
  def PutArrayOfBytesToJar(ba: Array[Byte], jarName: String) = {
    logger.info("Downloading the jar contents into the file " + jarName)
    try {
      val iFile = new File(jarName)
      val bos = new BufferedOutputStream(new FileOutputStream(iFile));
      bos.write(ba)
      bos.close();
    } catch {
      case e: Exception => {
        logger.error("Failed to dump array of bytes to the Jar file (" + jarName + "):  " + e.getMessage(), e)
      }
    }
  }

    /**
     * UploadJarsToDB
     * @param obj <description please>
     * @param forceUploadMainJar <description please>
     * @param alreadyCheckedJars <description please>
    */
  def UploadJarsToDB(obj: BaseElemDef, forceUploadMainJar: Boolean = true, alreadyCheckedJars: scala.collection.mutable.Set[String] = null): Unit = {
    val checkedJars: scala.collection.mutable.Set[String] = if (alreadyCheckedJars == null) scala.collection.mutable.Set[String]() else alreadyCheckedJars

    try {
      var keyList = new ArrayBuffer[String](0)
      var valueList = new ArrayBuffer[Array[Byte]](0)

      val tmpJarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS")
      val jarPaths = if (tmpJarPaths != null) tmpJarPaths.split(",").toSet else scala.collection.immutable.Set[String]()
      if (obj.jarName != null && (forceUploadMainJar || checkedJars.contains(obj.jarName) == false)) {
        //BUGBUG
        val jarsPathsInclTgtDir = jarPaths + MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR")
        var jarName = com.ligadata.Utils.Utils.GetValidJarFile(jarsPathsInclTgtDir, obj.jarName)
        var value = GetJarAsArrayOfBytes(jarName)

        var loadObject = false

        if (forceUploadMainJar) {
          loadObject = true
        } else {
          var mObj: Value = null
          try {
            mObj = GetObject(obj.jarName, "jar_store")
          } catch {
            case e: ObjectNotFoundException => {
              logger.debug("", e)
              loadObject = true
            }
            case e: Exception => {
              logger.debug("", e)
              loadObject = true
            }
          }

          if (loadObject == false) {
            val ba = mObj.serializedInfo
            val fs = ba.length
            if (fs != value.length) {
              logger.debug("A jar file already exists, but it's size (" + fs + ") doesn't match with the size of the Jar (" +
                jarName + "," + value.length + ") of the object(" + obj.FullNameWithVer + ")")
              loadObject = true
            }
          }
        }

        checkedJars += obj.jarName

        if (loadObject) {
          logger.debug("Update the jarfile (size => " + value.length + ") of the object: " + obj.jarName)
          keyList += obj.jarName
          valueList += value
        }
      }

      if (obj.DependencyJarNames != null) {
        obj.DependencyJarNames.foreach(j => {
          // do not upload if it already exist & just uploaded/checked in db, minor optimization
          if (j.endsWith(".jar") && checkedJars.contains(j) == false) {
            var loadObject = false
            val jarName = com.ligadata.Utils.Utils.GetValidJarFile(jarPaths, j)
            val value = GetJarAsArrayOfBytes(jarName)
            var mObj: Value = null
            try {
              mObj = GetObject(j, "jar_store")
            } catch {
              case e: ObjectNotFoundException => {
                logger.debug("", e)
                loadObject = true
              }
            }

            if (loadObject == false) {
              val ba = mObj.serializedInfo
              val fs = ba.length
              if (fs != value.length) {
                logger.debug("A jar file already exists, but it's size (" + fs + ") doesn't match with the size of the Jar (" +
                  jarName + "," + value.length + ") of the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + ")")
                loadObject = true
              }
            }

            if (loadObject) {
              keyList += j
              logger.debug("Update the jarfile (size => " + value.length + ") of the object: " + j)
              valueList += value
            } else {
              logger.debug("The jarfile " + j + " already exists in DB.")
            }
            checkedJars += j
          }
        })
      }
      if (keyList.length > 0) {
        SaveObjectList(keyList.toArray, valueList.toArray, "jar_store", "")
      }
    } catch {
      case e: Exception => {
        logger.debug("", e)
        throw InternalErrorException("Failed to Update the Jar of the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + ")", e)
      }
    }
  }

    /**
     * UploadJarToDB
     * @param jarName <description please>
     */
  def UploadJarToDB(jarName: String) {
    try {
      val f = new File(jarName)
      if (f.exists()) {
        var key = f.getName()
        var value = GetJarAsArrayOfBytes(jarName)
        logger.debug("Update the jarfile (size => " + value.length + ") of the object: " + jarName)
        SaveObject(key, value, "jar_store", "")

        var apiResult = new ApiResult(ErrorCodeConstants.Success, "UploadJarToDB", null, ErrorCodeConstants.Upload_Jar_Successful + ":" + jarName)
        apiResult.toString()

      }
    } catch {
      case e: Exception => {
        logger.debug("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadJarToDB", null, "Error : " + e.toString() + ErrorCodeConstants.Upload_Jar_Failed + ":" + jarName)
        apiResult.toString()
      }
    }
  }

    /**
     * UploadJarToDB
     * @param jarName <description please>
     * @param byteArray <description please>
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return <description please>
     */
  def UploadJarToDB(jarName: String, byteArray: Array[Byte], userid: Option[String] = None): String = {
    try {
      var key = jarName
      var value = byteArray
      logger.debug("Update the jarfile (size => " + value.length + ") of the object: " + jarName)
      logAuditRec(userid, Some(AuditConstants.WRITE), AuditConstants.INSERTJAR, jarName, AuditConstants.SUCCESS, "", jarName)
      SaveObject(key, value, "jar_store", "")
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "UploadJarToDB", null, ErrorCodeConstants.Upload_Jar_Successful + ":" + jarName)
      apiResult.toString()
    } catch {
      case e: Exception => {
        logger.debug("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadJarToDB", null, "Error : " + e.toString() + ErrorCodeConstants.Upload_Jar_Failed + ":" + jarName)
        apiResult.toString()
      }
    }
  }

    /**
     * IsDownloadNeeded
     * @param jar <description please>
     * @param obj <description please>
     * @return <description please>
     */
  def IsDownloadNeeded(jar: String, obj: BaseElemDef): Boolean = {
    try {
      if (jar == null) {
        logger.debug("The object " + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + " has no jar associated with it. Nothing to download..")
        false
      }
      val tmpJarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS")
      val jarPaths = if (tmpJarPaths != null) tmpJarPaths.split(",").toSet else scala.collection.immutable.Set[String]()
      val jarName = com.ligadata.Utils.Utils.GetValidJarFile(jarPaths, jar)
      val f = new File(jarName)
      if (f.exists()) {
        val key = jar
        val mObj = GetObject(key, "jar_store")
        val ba = mObj.serializedInfo
        val fs = f.length()
        if (fs != ba.length) {
          logger.debug("A jar file already exists, but it's size (" + fs + ") doesn't match with the size of the Jar (" +
            jar + "," + ba.length + ") of the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + ")")
          true
        } else {
          logger.debug("A jar file already exists, and it's size (" + fs + ")  matches with the size of the existing Jar (" +
            jar + "," + ba.length + ") of the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "), no need to download.")
          false
        }
      } else {
        logger.debug("The jar " + jarName + " is not available, download from database. ")
        true
      }
    } catch {
      case e: ObjectNotFoundException => {
        logger.debug("", e)
        true
      }
      case e: Exception => {
        logger.debug("", e)
        throw InternalErrorException("Failed to verify whether a download is required for the jar " + jar + " of the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + ")", e)
      }
    }
  }

    /**
     * GetDependantJars of some base element (e.g., model, type, message, container, etc)
     * @param obj <description please>
     * @return <description please>
     */
  def GetDependantJars(obj: BaseElemDef): Array[String] = {
    try {
      var allJars = new Array[String](0)
      if (obj.JarName != null)
        allJars = allJars :+ obj.JarName
      if (obj.DependencyJarNames != null) {
        obj.DependencyJarNames.foreach(j => {
          if (j.endsWith(".jar")) {
            allJars = allJars :+ j
          }
        })
      }
      allJars
    } catch {
      case e: Exception => {
        logger.debug("", e)
        throw InternalErrorException("Failed to get dependant jars for the given object (" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + ")", e)
      }
    }
  }

    /**
     * DownloadJarFromDB
     * @param obj <description please>
     */
  def DownloadJarFromDB(obj: BaseElemDef) {
    var curJar: String = ""
    try {
      //val key:String = (getObjectType(obj) + "." + obj.FullNameWithVer).toLowerCase
      if (obj.jarName == null) {
        logger.debug("The object " + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + " has no jar associated with it. Nothing to download..")
        return
      }
      var allJars = GetDependantJars(obj)
      logger.debug("Found " + allJars.length + " dependent jars. Jars:" + allJars.mkString(","))
      logger.info("Found " + allJars.length + " dependent jars. It make take several minutes first time to download all of these jars:" + allJars.mkString(","))
      if (allJars.length > 0) {
        val tmpJarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS")
        val jarPaths = if (tmpJarPaths != null) tmpJarPaths.split(",").toSet else scala.collection.immutable.Set[String]()
        jarPaths.foreach(jardir => {
          val dir = new File(jardir)
          if (!dir.exists()) {
            // attempt to create the missing directory
            dir.mkdir();
          }
        })

        val dirPath = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_TARGET_DIR")
        val dir = new File(dirPath)
        if (!dir.exists()) {
          // attempt to create the missing directory
          dir.mkdir();
        }

        allJars.foreach(jar => {
          curJar = jar
          try {
            // download only if it doesn't already exists
            val b = IsDownloadNeeded(jar, obj)
            if (b == true) {
              val key = jar
              val mObj = GetObject(key, "jar_store")
              val ba = mObj.serializedInfo
              val jarName = dirPath + "/" + jar
              PutArrayOfBytesToJar(ba, jarName)
            } else {
              logger.debug("The jar " + curJar + " was already downloaded... ")
            }
          } catch {
            case e: Exception => {
              logger.error("Failed to download the Jar of the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "'s dep jar " + curJar + ")", e)

            }
          }
        })
      }
    } catch {
      case e: Exception => {
        logger.error("Failed to download the Jar of the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "'s dep jar " + curJar + ")", e)

      }
    }
  }

    /**
     * UpdateObjectInCache
     * @param obj <description please>
     * @param operation depending upon object type, operations to add, remove, et al
     * @param mdMgr the metadata manager receiver 
     * @return <description please>
     */
  def UpdateObjectInCache(obj: BaseElemDef, operation: String, mdMgr: MdMgr): BaseElemDef = {
    var updatedObject: BaseElemDef = null

    // Update the current transaction level with this object  ???? What if an exception occurs ????
    if (currentTranLevel < obj.TranId) currentTranLevel = obj.TranId

    try {
      obj match {
        case o: FunctionDef => {
          updatedObject = mdMgr.ModifyFunction(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ModelDef => {
          updatedObject = mdMgr.ModifyModel(o.nameSpace, o.name, o.ver, operation)
        }
        case o: MessageDef => {
          updatedObject = mdMgr.ModifyMessage(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ContainerDef => {
          updatedObject = mdMgr.ModifyContainer(o.nameSpace, o.name, o.ver, operation)
        }
        case o: AttributeDef => {
          updatedObject = mdMgr.ModifyAttribute(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ScalarTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ArrayTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ArrayBufTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ListTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: QueueTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: SetTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: TreeSetTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: SortedSetTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: MapTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ImmutableMapTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: HashMapTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: TupleTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: ContainerTypeDef => {
          updatedObject = mdMgr.ModifyType(o.nameSpace, o.name, o.ver, operation)
        }
        case o: OutputMsgDef => {
          updatedObject = mdMgr.ModifyOutputMsg(o.nameSpace, o.name, o.ver, operation)
        }
        case _ => {
          throw InternalErrorException("UpdateObjectInCache is not implemented for objects of type " + obj.getClass.getName, null)
        }
      }
      updatedObject
    } catch {
      case e: ObjectNolongerExistsException => {
        logger.debug("", e)
        throw ObjectNolongerExistsException("The object " + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + " nolonger exists in metadata : It may have been removed already", e)
      }
      case e: Exception => {
        logger.debug("", e)
        throw e
      }
    }
  }

  // For now only handle the Model COnfig... Engine Configs will come later
    /**
     * AddConfigObjToCache
     * @param tid <description please>
     * @param key <description please>
     * @param mdlConfig <description please>
     *  @param mdMgr the metadata manager receiver
     */
  def AddConfigObjToCache(tid: Long, key: String, mdlConfig: Map[String, List[String]], mdMgr: MdMgr) {
    // Update the current transaction level with this object  ???? What if an exception occurs ????
    if (currentTranLevel < tid) currentTranLevel = tid
    try {
      mdMgr.AddModelConfig(key, mdlConfig)
    } catch { //Map[String, List[String]]
      case e: AlreadyExistsException => {
        logger.error("Failed to Cache the config object(" + key + ")", e)
      }
      case e: Exception => {
        logger.error("Failed to Cache the config object(" + key + ")", e)
      }
    }
  }

    /**
     * AddObjectToCache
     * @param o <description please>
     *  @param mdMgr the metadata manager receiver
     */
  def AddObjectToCache(o: Object, mdMgr: MdMgr) {
    // If the object's Delete flag is set, this is a noop.
    val obj = o.asInstanceOf[BaseElemDef]

    // Update the current transaction level with this object  ???? What if an exception occurs ????
    if (currentTranLevel < obj.TranId) currentTranLevel = obj.TranId

    if (obj.IsDeleted)
      return
    try {
      val key = obj.FullNameWithVer.toLowerCase
      val dispkey = obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version)
      obj match {
        case o: ModelDef => {
          logger.debug("Adding the model to the cache: name of the object =>  " + dispkey)
          mdMgr.AddModelDef(o, true)
        }
        case o: MessageDef => {
          logger.debug("Adding the message to the cache: name of the object =>  " + dispkey)
          mdMgr.AddMsg(o)
        }
        case o: ContainerDef => {
          logger.debug("Adding the container to the cache: name of the object =>  " + dispkey)
          mdMgr.AddContainer(o)
        }
        case o: FunctionDef => {
          val funcKey = o.typeString.toLowerCase
          logger.debug("Adding the function to the cache: name of the object =>  " + funcKey)
          mdMgr.AddFunc(o)
        }
        case o: AttributeDef => {
          logger.debug("Adding the attribute to the cache: name of the object =>  " + dispkey)
          mdMgr.AddAttribute(o)
        }
        case o: ScalarTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddScalar(o)
        }
        case o: ArrayTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddArray(o)
        }
        case o: ArrayBufTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddArrayBuffer(o)
        }
        case o: ListTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddList(o)
        }
        case o: QueueTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddQueue(o)
        }
        case o: SetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddSet(o)
        }
        case o: TreeSetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddTreeSet(o)
        }
        case o: SortedSetTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddSortedSet(o)
        }
        case o: MapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddMap(o)
        }
        case o: ImmutableMapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddImmutableMap(o)
        }
        case o: HashMapTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddHashMap(o)
        }
        case o: TupleTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddTupleType(o)
        }
        case o: ContainerTypeDef => {
          logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          mdMgr.AddContainerType(o)
        }
        case o: OutputMsgDef => {
          logger.trace("Adding the Output Msg to the cache: name of the object =>  " + key)
          mdMgr.AddOutputMsg(o)
        }
        case _ => {
          logger.error("SaveObject is not implemented for objects of type " + obj.getClass.getName)
        }
      }
    } catch {
      case e: AlreadyExistsException => {
        logger.error("Already Exists! Failed to Cache the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + ")", e)
      }
      case e: Exception => {
        logger.error("Exception! Failed to Cache the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + ")", e)
      }
    }
  }

    /**
     * ModifyObject
     * @param obj
     * @param operation
     */
  def ModifyObject(obj: BaseElemDef, operation: String) {
    try {
      val o1 = UpdateObjectInCache(obj, operation, MdMgr.GetMdMgr)
      UpdateObjectInDB(o1)
    } catch {
      case e: ObjectNolongerExistsException => {
        logger.error("The object " + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + " nolonger exists in metadata : It may have been removed already", e)
      }
      case e: Exception => {
        logger.debug("", e)
        throw new Exception("Unexpected error in ModifyObject", e)
      }
    }
  }

    /**
     * DeleteObject
     * @param key
     * @param typeName
     */
  def DeleteObject(bucketKeyStr: String, typeName: String) {
    val (containerName, store) = tableStoreMap(typeName)
    store.del(containerName, Array(Key(storageDefaultTime, Array(bucketKeyStr), storageDefaultTxnId, 0)))
  }

    /**
     * DeleteObject
     * @param obj
     */
  def DeleteObject(obj: BaseElemDef) {
    try {
      ModifyObject(obj, "Remove")
    } catch {
      case e: ObjectNolongerExistsException => {
        logger.error("The object " + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + " nolonger exists in metadata : It may have been removed already", e)
      }
      case e: Exception => {
        logger.debug("", e)
        throw new Exception("Unexpected error in DeleteObject", e)
      }
    }
  }

    /**
     * ActivateObject
     * @param obj
     */
  def ActivateObject(obj: BaseElemDef) {
    try {
      ModifyObject(obj, "Activate")
    } catch {
      case e: ObjectNolongerExistsException => {
        logger.debug("The object " + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + " nolonger exists in metadata : It may have been removed already", e)
      }
      case e: Exception => {
        logger.debug("", e)
        throw new Exception("Unexpected error in ActivateObject", e)
      }
    }
  }

    /**
     * DeactivateObject
     * @param obj
     */
  def DeactivateObject(obj: BaseElemDef) {
    try {
      ModifyObject(obj, "Deactivate")
    } catch {
      case e: ObjectNolongerExistsException => {
        logger.error("The object " + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + " nolonger exists in metadata : It may have been removed already", e)
      }
      case e: Exception => {
        logger.debug("", e)
        throw new Exception("Unexpected error in DeactivateObject", e)
      }
    }
  }

    /**
     * getApiResult
     * @param apiResultJson
     * @return
     */
  @throws(classOf[Json4sParsingException])
  @throws(classOf[ApiResultParsingException])
  def getApiResult(apiResultJson: String): String = {
    // parse using Json4s
    try {
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(apiResultJson)
      //logger.debug("Parsed the json : " + apiResultJson)
      val apiResultInfo = json.extract[APIResultJsonProxy]
      (apiResultInfo.APIResults.statusCode + apiResultInfo.APIResults.functionName + apiResultInfo.APIResults.resultData + apiResultInfo.APIResults.description)
    } catch {
      case e: MappingException => {
        logger.debug("", e)
        throw Json4sParsingException(e.getMessage(), e)
      }
      case e: Exception => {
        logger.debug("", e)
        throw ApiResultParsingException(e.getMessage(), e)
      }
    }
  }

    /**
     * GetDataStoreHandle
     * @param jarPaths Set of paths where jars are located Set of paths where jars are located
     * @param dataStoreInfo information needed to access the data store (kv store dependent)
     * @return
     */
  private def GetDataStoreHandle(jarPaths: collection.immutable.Set[String], dataStoreInfo: String): DataStore = {
  //private def GetDataStoreHandle(jarPaths: collection.immutable.Set[String], dataStoreInfo: String, tableName: String): DataStore = {
    try {
      logger.debug("Getting DB Connection for dataStoreInfo:%s".format(dataStoreInfo))
      return KeyValueManager.Get(jarPaths, dataStoreInfo)
    } catch {
      case e: Exception => {
        logger.debug("", e)
        throw new CreateStoreFailedException(e.getMessage(), e)
      }
    }
  }

    /**
     * OpenDbStore
     * @param jarPaths Set of paths where jars are located
     * @param dataStoreInfo information needed to access the data store (kv store dependent)
     */
  def OpenDbStore(jarPaths: collection.immutable.Set[String], dataStoreInfo: String) {
    try {
      logger.debug("Opening datastore")
      mainDS = GetDataStoreHandle(jarPaths, dataStoreInfo)

      tableStoreMap = Map("metadata_objects" -> ("metadata_objects", mainDS),
        "models" -> ("metadata_objects", mainDS),
        "messages" -> ("metadata_objects", mainDS),
        "containers" -> ("metadata_objects", mainDS),
        "functions" -> ("metadata_objects", mainDS),
        "concepts" -> ("metadata_objects", mainDS),
        "types" -> ("metadata_objects", mainDS),
        "others" -> ("metadata_objects", mainDS),
        "outputmsgs" -> ("metadata_objects", mainDS),
        "jar_store" -> ("jar_store", mainDS),
        "config_objects" -> ("config_objects", mainDS),
        "model_config_objects" -> ("model_config_objects", mainDS),
        "transaction_id" -> ("transaction_id", mainDS))
    } catch {
      case e: FatalAdapterException => {
        logger.error("Failed to connect to Datastore", e)
        throw CreateStoreFailedException(e.getMessage(), e)
      }
      case e: StorageConnectionException => {
        logger.error("Failed to connect to Datastore", e)
        throw CreateStoreFailedException(e.getMessage(), e)
      }
      case e: StorageFetchException => {
        logger.error("Failed to connect to Datastore", e)
        throw CreateStoreFailedException(e.getMessage(), e)
      }
      case e: StorageDMLException => {
        logger.error("Failed to connect to Datastore", e)
        throw CreateStoreFailedException(e.getMessage(), e)
      }
      case e: StorageDDLException => {
        logger.error("Failed to connect to Datastore", e)
        throw CreateStoreFailedException(e.getMessage(), e)
      }
      case e: Exception => {
        logger.error("Failed to connect to Datastore", e)
        throw CreateStoreFailedException(e.getMessage(), e)
      }
      case e: Throwable => {
        logger.error("Failed to connect to Datastore", e)
        throw CreateStoreFailedException(e.getMessage(), e)
      }
    }
  }

    /**
     * CloseDbStore
     */
  def CloseDbStore: Unit = lock.synchronized {
    try {
      logger.debug("Closing datastore")
      if (mainDS != null) {
        mainDS.Shutdown()
        mainDS = null
        logger.debug("main datastore closed")
      }
    } catch {
      case e: Exception => {
        logger.error("", e)
        throw e;
      }
    }
  }

    /**
     * TruncateDbStore
     */
  def TruncateDbStore: Unit = lock.synchronized {
    try {
      logger.debug("Not allowing to truncate the whole datastore")
      // mainDS.TruncateStore
    } catch {
      case e: Exception => {
        logger.error("", e)
        throw e;
      }
    }
  }

    /**
     * TruncateAuditStore
     */
  def TruncateAuditStore: Unit = lock.synchronized {
    try {
      logger.debug("Truncating Audit datastore")
      if (auditObj != null) {
        auditObj.TruncateStore
      }
    } catch {
      case e: Exception => {
        logger.error("", e)
        throw e;
      }
    }
  }

    /**
     * AddType
     * @param typeText
     * @param format
     * @return
     */
  def AddType(typeText: String, format: String, userid: Option[String] = None): String = {
    TypeUtils.AddType(typeText,format)
  }

    /**
     * AddType
     * @param typeDef
     * @return
     */
  def AddType(typeDef: BaseTypeDef): String = {
    TypeUtils.AddType(typeDef)
  }

    /**
     * AddTypes
     * @param typesText
     * @param format
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def AddTypes(typesText: String, format: String, userid: Option[String] = None): String = {
    TypeUtils.AddTypes(typesText,format,userid)
  }

  /**
    * Remove type for given TypeName and Version
    * @param typeNameSpace
    * @param typeName name of the Type
    * @param version  Version of the object
    * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
    *               method. If Security and/or Audit are configured, this value must be a value other than None.
    * @return the result as a JSON String of object ApiResult where ApiResult.statusCode
    *         indicates success or failure of operation: 0 for success, Non-zero for failure. The Value of
    *         ApiResult.statusDescription and ApiResult.resultData indicate the nature of the error in case of failure
    *
    *         Example:
    *
    *         {{{
    *          val apiResult = MetadataAPIImpl.RemoveType(MdMgr.sysNS,"my_char",100)
    *          val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
    *          println("Result as Json String => \n" + resultData)
    *          }}}
    *
    */
  def RemoveType(typeNameSpace: String, typeName: String, version: Long, userid: Option[String] = None): String = {
    TypeUtils.RemoveType(typeNameSpace,typeName,version,userid)
  }

   /**
    * UpdateType
    * @param typeJson
    * @param format
    * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
    *               method. If Security and/or Audit are configured, this value must be a value other than None.
    * @return the result as a JSON String of object ApiResult where ApiResult.statusCode
    *         indicates success or failure of operation: 0 for success, Non-zero for failure. The Value of
    *         ApiResult.statusDescription and ApiResult.resultData indicate the nature of the error in case of failure
    *
    *         Example:
    *
    *         {{{
    *          val sampleScalarTypeStr = """
    *          {
    * "MetadataType" : "ScalarTypeDef",
    * "NameSpace" : "system",
    * "Name" : "my_char",
    * "TypeTypeName" : "tScalar",
    * "TypeNameSpace" : "System",
    * "TypeName" : "Char",
    * "PhysicalName" : "Char",
    * "Version" : 101,
    * "JarName" : "basetypes_2.10-0.1.0.jar",
    * "DependencyJars" : [ "metadata_2.10-1.0.jar" ],
    * "Implementation" : "com.ligadata.BaseTypes.CharImpl"
    * }
    * """
    * var apiResult = MetadataAPIImpl.UpdateType(sampleScalarTypeStr,"JSON")
    * var result = MetadataAPIImpl.getApiResult(apiResult)
    * println("Result as Json String => \n" + result._2)
    * }}}
    *
    */
  def UpdateType(typeJson: String, format: String, userid: Option[String] = None): String = {
    TypeUtils.UpdateType(typeJson,format,userid)
  }

  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    /**
     * Upload Jars into system. Dependency jars may need to upload first. Once we upload the jar, if we retry to upload it will throw an exception.
     * @param jarPath where the jars are
     * @return
     */
  def UploadJar(jarPath: String, userid: Option[String] = None): String = {
    try {
      val iFile = new File(jarPath)
      if (!iFile.exists) {
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadJar", null, ErrorCodeConstants.File_Not_Found + ":" + jarPath)
        apiResult.toString()
      } else {
        val jarName = iFile.getName()
        val jarObject = MdMgr.GetMdMgr.MakeJarDef(MetadataAPIImpl.sysNS, jarName, "100")


        logger.debug(" UploadJar  ==>>    ===>> " + jarPath )
        jarObject.tranId = GetNewTranId

        var objectsAdded = new Array[BaseElemDef](0)
        objectsAdded = objectsAdded :+ jarObject
        UploadJarToDB(jarPath)
        val operations = for (op <- objectsAdded) yield "Add"
        NotifyEngine(objectsAdded, operations)
        val apiResult = new ApiResult(ErrorCodeConstants.Success, "UploadJar", null, ErrorCodeConstants.Upload_Jar_Successful + ":" + jarPath)
        apiResult.toString()
      }
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.debug(stackTrace)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "UploadJar", null, "Error :" + e.toString() + ErrorCodeConstants.Upload_Jar_Failed + ":" + jarPath + "\nStackTrace:" + stackTrace)
        apiResult.toString()
      }
    }
  }

    /** '
      * AddDerivedConcept
      * @param conceptsText
      * @param format
      * @return
      */
  def AddDerivedConcept(conceptsText: String, format: String): String = {
    ConceptUtils.AddDerivedConcept(conceptsText, format)
  }

    /**
    * AddConcepts
    * @param conceptsText an input String of concepts in a format defined by the next parameter formatType
    * @param format
    * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
    *               method. If Security and/or Audit are configured, this value must be a value other than None.
    * @return the result as a JSON String of object ApiResult where ApiResult.statusCode
    *         indicates success or failure of operation: 0 for success, Non-zero for failure. The Value of
    *         ApiResult.statusDescription and ApiResult.resultData indicate the nature of the error in case of failure
    *
    *         Example:
    *
    *         {{{
    *            val sampleConceptStr = """
    *           {"Concepts" : [
    *  "NameSpace":"Ligadata",
    *  "Name":"ProviderId",
    *  "TypeNameSpace":"System",
    *  "TypeName" : "String",
    *  "Version"  : 100 ]
    *  }
    *"""
    *    var apiResult = MetadataAPIImpl.AddConcepts(sampleConceptStr,"JSON")
    *    var result = MetadataAPIImpl.getApiResult(apiResult)
    *    println("Result as Json String => \n" + result._2)
    *}}}
    *
    */
  def AddConcepts(conceptsText: String, format: String, userid: Option[String] = None): String = {
    ConceptUtils.AddConcepts(conceptsText,format,userid)
  }

    /**
    * UpdateConcepts
    * @param conceptsText an input String of concepts in a format defined by the next parameter formatType
    * @param format
    * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
    *               method. If Security and/or Audit are configured, this value must be a value other than None.
    * @return the result as a JSON String of object ApiResult where ApiResult.statusCode
    *         indicates success or failure of operation: 0 for success, Non-zero for failure. The Value of
    *         ApiResult.statusDescription and ApiResult.resultData indicate the nature of the error in case of failure
    *
    *         Example:
    *
    *         {{{
    *            val sampleConceptStr = """
    *           {"Concepts" : [
    *  "NameSpace":"Ligadata",
    *  "Name":"ProviderId",
    *  "TypeNameSpace":"System",
    *  "TypeName" : "String",
    *  "Version"  : 101 ]
    *  }
    *"""
    *    var apiResult = MetadataAPIImpl.UpdateConcepts(sampleConceptStr,"JSON")
    *    var result = MetadataAPIImpl.getApiResult(apiResult)
    *    println("Result as Json String => \n" + result._2)
    *
    *}}}
    *
    */
  def UpdateConcepts(conceptsText: String, format: String, userid: Option[String] = None): String = {
    ConceptUtils.UpdateConcepts(conceptsText,format,userid)
  }

    /**
     * RemoveConcept
     * @param key
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def RemoveConcept(key: String, userid: Option[String] = None): String = {
    ConceptUtils.RemoveConcept(key,userid)
  }

    /**
     * RemoveConcept
     * @param nameSpace namespace of the object
     * @param name
     * @param version  Version of the object
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def RemoveConcept(nameSpace: String, name: String, version: Long, userid: Option[String]): String = {
    ConceptUtils.RemoveConcept(nameSpace, name, version, userid)
  }

    /**
     * RemoveConcepts take all concepts names to be removed as an Array
     * @param concepts array of Strings where each string is name of the concept
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return the result as a JSON String of object ApiResult where ApiResult.statusCode
     *         indicates success or failure of operation: 0 for success, Non-zero for failure. The Value of
     *         ApiResult.statusDescription and ApiResult.resultData indicate the nature of the error in case of failure
     *
     *         Example:
     *         {{{
     *          val apiResult = MetadataAPIImpl.RemoveConcepts(Array("Ligadata.ProviderId.100"))
     *          val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
     *          println("Result as Json String => \n" + resultData)
     *         }}}
     *
     */

  def RemoveConcepts(concepts: Array[String], userid: Option[String] = None): String = {
    ConceptUtils.RemoveConcepts(concepts,userid)
  }

    /**
     * AddContainerDef
     * @param contDef
     * @param recompile
     * @return
     */
  def AddContainerDef(contDef: ContainerDef, recompile: Boolean = false): String = {
    MessageAndContainerUtils.AddContainerDef(contDef,recompile)
  }

    /**
     * AddMessageDef
     * @param msgDef
     * @param recompile
     * @return
     */
  def AddMessageDef(msgDef: MessageDef, recompile: Boolean = false): String = {
    MessageAndContainerUtils.AddMessageDef(msgDef,recompile)
  }

    /**
     * AddMessageTypes
     * @param msgDef
     * @param mdMgr the metadata manager receiver
     * @param recompile
     * @return
     */
  def AddMessageTypes(msgDef: BaseElemDef, mdMgr: MdMgr, recompile: Boolean = false): Array[BaseElemDef] = {
    MessageAndContainerUtils.AddMessageTypes(msgDef,mdMgr,recompile)
  }

    /**
     * AddContainerOrMessage
     * @param contOrMsgText message
     * @param format its format
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @param recompile a
     * @return <description please>
     */
  private def AddContainerOrMessage(contOrMsgText: String, format: String, userid: Option[String], recompile: Boolean = false): String = {
    MessageAndContainerUtils.AddContainerOrMessage(contOrMsgText,format,userid,recompile)
  }

    /**
     * AddMessage
     * @param messageText text of the message (as JSON/XML string as defined by next parameter formatType)
     * @param format
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return the result as a JSON String of object ApiResult where ApiResult.statusCode
     *         indicates success or failure of operation: 0 for success, Non-zero for failure. The Value of
     *         ApiResult.statusDescription and ApiResult.resultData indicate the nature of the error in case of failure
     *
     *         Example
     *
     *         {{{
     *          var apiResult = MetadataAPIImpl.AddMessage(msgStr,"JSON")
     *          var result = MetadataAPIImpl.getApiResult(apiResult)
     *          println("Result as Json String => \n" + result._2)
     *          }}}
     */
  override def AddMessage(messageText: String, format: String, userid: Option[String] = None): String = {
    AddContainerOrMessage(messageText, format, userid)
  }

    /**
    * AddContainer
    * @param containerText text of the container (as JSON/XML string as defined by next parameter formatType)
    * @param format
    * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
    *               method. If Security and/or Audit are configured, this value must be a value other than None.
    * @return the result as a JSON String of object ApiResult where ApiResult.statusCode
    *         indicates success or failure of operation: 0 for success, Non-zero for failure. The Value of
    *         ApiResult.statusDescription and ApiResult.resultData indicate the nature of the error in case of failure
    *
    *         Example
    *
    *         {{{
    *          var apiResult = MetadataAPIImpl.AddContainer(msgStr,"JSON")
    *          var result = MetadataAPIImpl.getApiResult(apiResult)
    *          println("Result as Json String => \n" + result._2)
    *          }}}
    */
  def AddContainer(containerText: String, format: String, userid: Option[String] = None): String = {
    AddContainerOrMessage(containerText, format, userid)
  }

    /**
     * AddContainer
     * @param containerText
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def AddContainer(containerText: String, userid: Option[String]): String = {
    AddContainer(containerText, "JSON", userid)
  }

    /**
     * RecompileMessage
     * @param msgFullName
     * @return
     */
  def RecompileMessage(msgFullName: String): String = {
    MessageAndContainerUtils.RecompileMessage(msgFullName)
  }

    /**
     * UpdateMessage
     * @param messageText text of the message (as JSON/XML string as defined by next parameter formatType)
     * @param format
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return the result as a JSON String of object ApiResult where ApiResult.statusCode
     *         indicates success or failure of operation: 0 for success, Non-zero for failure. The Value of
     *         ApiResult.statusDescription and ApiResult.resultData indicate the nature of the error in case of failure
     */
  def UpdateMessage(messageText: String, format: String, userid: Option[String] = None): String = {
    MessageAndContainerUtils.UpdateMessage(messageText,format,userid)
  }

    /**
     * UpdateContainer
     * @param messageText
     * @param format
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return the result as a JSON String of object ApiResult where ApiResult.statusCode
     *         indicates success or failure of operation: 0 for success, Non-zero for failure. The Value of
     *         ApiResult.statusDescription and ApiResult.resultData indicate the nature of the error in case of failure
     */
  def UpdateContainer(messageText: String, format: String, userid: Option[String] = None): String = {
    UpdateMessage(messageText, format, userid)
  }

    /**
     * UpdateContainer
     * @param messageText
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def UpdateContainer(messageText: String, userid: Option[String]): String = {
    UpdateMessage(messageText, "JSON", userid)
  }

    /**
     * UpdateMessage
     * @param messageText
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def UpdateMessage(messageText: String, userid: Option[String]): String = {
    UpdateMessage(messageText, "JSON", userid)
  }

    /**
     * Remove container with Container Name and Version Number
     * @param nameSpace namespace of the object
     * @param name
     * @param version  Version of the object
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @param zkNotify
     * @return
     */
  def RemoveContainer(nameSpace: String, name: String, version: Long, userid: Option[String], zkNotify: Boolean = true): String = {
    MessageAndContainerUtils.RemoveContainer(nameSpace,name,version,userid,zkNotify)   
  }

    /**
     * Remove message with Message Name and Version Number
     * @param nameSpace namespace of the object
     * @param name
     * @param version  Version of the object
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @param zkNotify
     * @return
     */
  def RemoveMessage(nameSpace: String, name: String, version: Long, userid: Option[String], zkNotify: Boolean = true): String = {
    MessageAndContainerUtils.RemoveMessage(nameSpace,name,version,userid,zkNotify)   
  }

    /**
     * When a message or container is compiled, the MetadataAPIImpl will automatically catalog an array, array buffer,
     * sorted set, immutable map of int array, array of array, et al where the message or container is a member element.
     * The type names are of the form <collectiontype>of<message type>.  Currently these container names are created:
     *
     *   {{{
     *       arrayof<message type>
     *       arraybufferof<message type>
     *       sortedsetof<message type>
     *       immutablemapofintarrayof<message type>
     *       immutablemapofstringarrayof<message type>
     *       arrayofarrayof<message type>
     *       mapofstringarrayof<message type>
     *       mapofintarrayof<message type>
     *       setof<message type>
     *       treesetof<message type>
     *   }}}
     * @param msgDef the name of the msgDef's type is used for the type name formation
     * @param mdMgr the metadata manager receiver
     * @return <description please>
     */
  def GetAdditionalTypesAdded(msgDef: BaseElemDef, mdMgr: MdMgr): Array[BaseElemDef] = {
    MessageAndContainerUtils.GetAdditionalTypesAdded(msgDef,mdMgr)
  }

    /**
     * Remove message with Message Name and Version Number based upon advice in supplied notification
     * @param zkMessage
     * @return
     */
  def RemoveMessageFromCache(zkMessage: ZooKeeperNotification) = {
    MessageAndContainerUtils.RemoveMessageFromCache(zkMessage)
  }

    /**
     * RemoveContainerFromCache
     * @param zkMessage
     * @return
     */
  def RemoveContainerFromCache(zkMessage: ZooKeeperNotification) = {
    MessageAndContainerUtils.RemoveContainerFromCache(zkMessage)
  }

    /**
     * Remove message with Message Name and Version Number
     * @param messageName Name of the given message
     * @param version  Version of the given message
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value should be other than None
     * @return the result as a JSON String of object ApiResult where ApiResult.statusCode
     *         indicates success or failure of operation: 0 for success, Non-zero for failure. The Value of
     *         ApiResult.statusDescription and ApiResult.resultData indicate the nature of the error in case of failure
     */
  def RemoveMessage(messageName: String, version: Long, userid: Option[String]): String = {
    RemoveMessage(sysNS, messageName, version, userid)
  }

   /**
    * Remove container with Container Name and Version Number
    * @param containerName Name of the given container
    * @param version  Version of the object   Version of the given container
    * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
    *               method. If Security and/or Audit are configured, this value must be a value other than None.
    * @return the result as a JSON String of object ApiResult where ApiResult.statusCode
    *         indicates success or failure of operation: 0 for success, Non-zero for failure. The Value of
    *         ApiResult.statusDescription and ApiResult.resultData indicate the nature of the error in case of failure
    */
  def RemoveContainer(containerName: String, version: Long, userid: Option[String]): String = {
    RemoveContainer(sysNS, containerName, version, userid)
  }

    /**
     * Deactivate the model that presumably is active and waiting for input in the working set of the cluster engines.
     * @param nameSpace namespace of the object
     * @param name
     * @param version  Version of the object
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def DeactivateModel(nameSpace: String, name: String, version: Long, userid: Option[String] = None): String = {
    val dispkey = nameSpace + "." + name + "." + MdMgr.Pad0s2Version(version)
    logAuditRec(userid, Some(AuditConstants.WRITE), AuditConstants.DEACTIVATEOBJECT, AuditConstants.MODEL, AuditConstants.SUCCESS, "", dispkey)
    if (DeactivateLocalModel(nameSpace, name, version)) {
      (new ApiResult(ErrorCodeConstants.Success, "Deactivate Model", null, ErrorCodeConstants.Deactivate_Model_Successful + ":" + dispkey)).toString
    } else {
      (new ApiResult(ErrorCodeConstants.Failure, "Deactivate Model", null, ErrorCodeConstants.Deactivate_Model_Failed_Not_Active + ":" + dispkey)).toString
    }
  }

    /**
     * Deactivate a model FIXME: Explain what it means to do this locally.
     * @param nameSpace namespace of the object
     * @param name
     * @param version  Version of the object
     * @return
     */
  private def DeactivateLocalModel(nameSpace: String, name: String, version: Long): Boolean = {
    var key = nameSpace + "." + name + "." + version
    val dispkey = nameSpace + "." + name + "." + MdMgr.Pad0s2Version(version)
    val newTranId = GetNewTranId
    try {
      val o = MdMgr.GetMdMgr.Model(nameSpace.toLowerCase, name.toLowerCase, version, true)
      o match {
        case None =>
          None
          logger.debug("No active model found => " + dispkey)
          false
        case Some(m) =>
          logger.debug("model found => " + m.asInstanceOf[ModelDef].FullName + "." + MdMgr.Pad0s2Version(m.asInstanceOf[ModelDef].Version))
          DeactivateObject(m.asInstanceOf[ModelDef])

          // TODO: Need to deactivate the appropriate message?
          m.tranId = newTranId
          var objectsUpdated = new Array[BaseElemDef](0)
          objectsUpdated = objectsUpdated :+ m.asInstanceOf[ModelDef]
          val operations = for (op <- objectsUpdated) yield "Deactivate"
          NotifyEngine(objectsUpdated, operations)
          true
      }
    } catch {
      case e: Exception => {
        logger.debug("", e)
        false
      }
    }
  }

    /**
     * Activate the model with the supplied keys. The engine is notified and the model factory is loaded.
     * @param nameSpace namespace of the object
     * @param name
     * @param version  Version of the object
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def ActivateModel(nameSpace: String, name: String, version: Long, userid: Option[String] = None): String = {
    var key = nameSpace + "." + name + "." + version
    val dispkey = nameSpace + "." + name + "." + MdMgr.Pad0s2Version(version)
    var currActiveModel: ModelDef = null
    val newTranId = GetNewTranId

    // Audit this call
    logAuditRec(userid, Some(AuditConstants.WRITE), AuditConstants.ACTIVATEOBJECT, AuditConstants.MODEL, AuditConstants.SUCCESS, "", nameSpace + "." + name + "." + version)

    try {
      // We may need to deactivate an model if something else is active.  Find the active model
      val oCur = MdMgr.GetMdMgr.Models(nameSpace, name, true, false)
      oCur match {
        case None =>
        case Some(m) =>
          var setOfModels = m.asInstanceOf[scala.collection.immutable.Set[ModelDef]]
          if (setOfModels.size > 1) {
            logger.error("Internal Metadata error, there are more then 1 versions of model " + nameSpace + "." + name + " active on this system.")
          }

          // If some model is active, deactivate it.
          if (setOfModels.size != 0) {
            currActiveModel = setOfModels.last
            if (currActiveModel.NameSpace.equalsIgnoreCase(nameSpace) &&
              currActiveModel.name.equalsIgnoreCase(name) &&
              currActiveModel.Version == version) {
              return (new ApiResult(ErrorCodeConstants.Success, "ActivateModel", null, dispkey + " already active")).toString

            }
            var isSuccess = DeactivateLocalModel(currActiveModel.nameSpace, currActiveModel.name, currActiveModel.Version)
            if (!isSuccess) {
              logger.error("Error while trying to activate " + dispkey + ", unable to deactivate active model. model ")
              val apiResult = new ApiResult(ErrorCodeConstants.Failure, "ActivateModel", null, "Error :" + ErrorCodeConstants.Activate_Model_Failed + ":" + dispkey + " -Unable to deactivate existing model")
              apiResult.toString()
            }
          }

      }

      // Ok, at this point, we have deactivate  a previously active model.. now we activate this one.
      val o = MdMgr.GetMdMgr.Model(nameSpace.toLowerCase, name.toLowerCase, version, false)
      o match {
        case None =>
          None
          logger.debug("No active model found => " + dispkey)
          val apiResult = new ApiResult(ErrorCodeConstants.Failure, "ActivateModel", null, ErrorCodeConstants.Activate_Model_Failed_Not_Active + ":" + dispkey)
          apiResult.toString()
        case Some(m) =>
          logger.debug("model found => " + m.asInstanceOf[ModelDef].FullName + "." + MdMgr.Pad0s2Version(m.asInstanceOf[ModelDef].Version))
          ActivateObject(m.asInstanceOf[ModelDef])

          // Issue a Notification to all registered listeners that an Acivation took place.
          // TODO: Need to activate the appropriate message?
          var objectsUpdated = new Array[BaseElemDef](0)
          m.tranId = newTranId
          objectsUpdated = objectsUpdated :+ m.asInstanceOf[ModelDef]
          val operations = for (op <- objectsUpdated) yield "Activate"
          NotifyEngine(objectsUpdated, operations)

          // No exceptions, we succeded
          val apiResult = new ApiResult(ErrorCodeConstants.Success, "ActivateModel", null, ErrorCodeConstants.Activate_Model_Successful + ":" + dispkey)
          apiResult.toString()
      }
    } catch {

      case e: Exception => {
        logger.debug("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "ActivateModel", null, "Error :" + e.toString() + ErrorCodeConstants.Activate_Model_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  }

    /**
     * Remove model with Model Name and Version Number
     * @param nameSpace namespace of the object
     * @param name
     * @param version  Version of the object
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  private def RemoveModel(nameSpace: String, name: String, version: Long, userid: Option[String]): String = {
    var key = nameSpace + "." + name + "." + version
    if (userid != None) logAuditRec(userid, Some(AuditConstants.WRITE), AuditConstants.DELETEOBJECT, "Model", AuditConstants.SUCCESS, "", key)
    val dispkey = nameSpace + "." + name + "." + MdMgr.Pad0s2Version(version)
    var newTranId = GetNewTranId
    try {
      val o = MdMgr.GetMdMgr.Model(nameSpace.toLowerCase, name.toLowerCase, version, true)
      o match {
        case None =>
          None
          logger.debug("model not found => " + dispkey)
          val apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveModel", null, ErrorCodeConstants.Remove_Model_Failed_Not_Found + ":" + dispkey)
          apiResult.toString()
        case Some(m) =>
          logger.debug("model found => " + m.asInstanceOf[ModelDef].FullName + "." + MdMgr.Pad0s2Version(m.asInstanceOf[ModelDef].Version))
          DeleteObject(m)
          var objectsUpdated = new Array[BaseElemDef](0)
          m.tranId = newTranId
          objectsUpdated = objectsUpdated :+ m
          var operations = for (op <- objectsUpdated) yield "Remove"
          NotifyEngine(objectsUpdated, operations)
          val apiResult = new ApiResult(ErrorCodeConstants.Success, "RemoveModel", null, ErrorCodeConstants.Remove_Model_Successful + ":" + dispkey)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        logger.debug("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "RemoveModel", null, "Error :" + e.toString() + ErrorCodeConstants.Remove_Model_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  }

   /**
    * Remove model with Model Name and Version Number
    * @param modelName the Namespace.Name of the given model to be removed
    * @param version   Version of the given model.  The version should comply with the Kamanja version format.  For example,
    *                  a value of "000001.000001.000001" shows the digits available for version.  All must be base 10 digits
    *                  with up to 6 digits for major version, minor version and micro version sections.
    *                  elper functions are available in MdMgr object for converting to/from strings and 0 padding the
    *                  version sections if desirable.
    * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
    *               method. If Security and/or Audit are configured, this value must be a value other than None.
    * @return the result as a JSON String of object ApiResult where ApiResult.statusCode
    *         indicates success or failure of operation: 0 for success, Non-zero for failure. The Value of
    *         ApiResult.statusDescription and ApiResult.resultData indicate the nature of the error in case of failure
    */
    override def RemoveModel(modelName: String, version: String, userid: Option[String] = None): String = {

        val reasonable : Boolean = modelName != null && modelName.length > 0
        val result : String = if (reasonable) {
            val buffer: StringBuilder = new StringBuilder
            val modelNameAdjusted: String = if (modelName.contains(".")) {
                modelName
            } else {
                logger.warn(s"No namespace qualification given...attempting removal with the ${sysNS} as the namespace")
                s"$sysNS.$modelName"
            }
            val modelNameNodes: Array[String] = modelNameAdjusted.split('.')
            val modelNm: String = modelNameNodes.last
            modelNameNodes.take(modelNameNodes.size - 1).addString(buffer, ".")
            val modelNmSpace: String = buffer.toString

            // old way; The Sytem namespace assumed... RemoveModel(sysNS, modelName, version, userid)

            RemoveModel(modelNmSpace, modelNm, MdMgr.ConvertVersionToLong(version), userid)

        } else {
            val modelNameStr : String = if (modelName == null) "NO MODEL NAME GIVEN" else "MODEL NAME of zero length"
            new ApiResult(ErrorCodeConstants.Failure, "RemoveModel", null, s"${ErrorCodeConstants.Remove_Model_Failed} : supplied model name ($modelNameStr) is bad").toString
        }
        result
    }


    /**
     * The ModelDef returned by the compilers is added to the metadata.
     * @param model
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured,supply something other than None
     * @return
     */
  def AddModel(model: ModelDef, userid : Option[String]): String = {
    var key = model.FullNameWithVer
    val dispkey = model.FullName + "." + MdMgr.Pad0s2Version(model.Version)
    try {
      SaveObject(model, MdMgr.GetMdMgr)
      val apiResult = new ApiResult(ErrorCodeConstants.Success, "AddModel", null, ErrorCodeConstants.Add_Model_Successful + ":" + dispkey)
      apiResult.toString()
    } catch {
      case e: Exception => {
        logger.debug("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Success, "AddModel", null, ErrorCodeConstants.Add_Model_Successful + ":" + dispkey)
        apiResult.toString()
      }
    }
  }

    /**
     * AddModelFromSource - compiles and catalogs a custom Scala or Java model from source.
     * @param sourceCode
     * @param sourceLang
     * @param modelName
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  private def AddModelFromSource(sourceCode: String, sourceLang: String, modelName: String, userid: Option[String] = None): String = {
    try {
      var compProxy = new CompilerProxy
      compProxy.setSessionUserId(userid)
      val modDef: ModelDef = compProxy.compileModelFromSource(sourceCode, modelName, sourceLang)
      logger.info("Begin uploading dependent Jars, please wait.")
      UploadJarsToDB(modDef)
      logger.info("Finished uploading dependent Jars.")
      val apiResult = AddModel(modDef, userid)

      // Add all the objects and NOTIFY the world
      var objectsAdded = new Array[BaseElemDef](0)
      objectsAdded = objectsAdded :+ modDef
      val operations = for (op <- objectsAdded) yield "Add"
      logger.debug("Notify engine via zookeeper")
      NotifyEngine(objectsAdded, operations)
      apiResult
    } catch {
      case e: AlreadyExistsException => {
        logger.debug("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, "Error : " + ErrorCodeConstants.Add_Model_Failed_Higher_Version_Required)
        apiResult.toString()
      }
      case e: MsgCompilationFailedException => {
        logger.debug("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, "Error : " + ErrorCodeConstants.Model_Compilation_Failed)
        apiResult.toString()
      }
      case e: Exception => {
        logger.error("Unknown compilation error occured", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, "Error : " + e.toString() + ErrorCodeConstants.Add_Model_Failed)
        apiResult.toString()
      }
    }
  }

    /** Add model. Several model types are currently supported.  They describe the content of the ''input'' argument:
      *
      *   - SCALA - a Scala source string
      *   - JAVA - a Java source string
      *   - PMML - a PMML source string
      *   - KPMML - a Kamanja Pmml source string
      *   - BINARY - the path to a jar containing the model
      *
      * The remaining arguments, while noted as optional, are required for some model types.  In particular,
      * the ''modelName'', ''version'', and ''msgConsumed'' must be specified for the PMML model type.  The ''userid'' is
      * required for systems that have been configured with a SecurityAdapter or AuditAdapter.
      * @see [[http://kamanja.org/security/ security wiki]] for more information. The audit adapter, if configured,
      *       will also be invoked to take note of this user's action.
      * @see [[http://kamanja.org/auditing/ auditing wiki]] for more information about auditing.
      * NOTE: The BINARY model is not supported at this time.  The model submitted for this type will via a jar file.
      *
      * @param modelType the type of the model submission (any {SCALA,JAVA,PMML,KPMML,BINARY}
      * @param input the text element to be added dependent upon the modelType specified.
      * @param optUserid the identity to be used by the security adapter to ascertain if this user has access permissions for this
      *               method.
      * @param optModelName the namespace.name of the PMML model to be added to the Kamanja metadata
      * @param optVersion the model version to be used to describe this PMML model
      * @param optMsgConsumed the namespace.name of the message to be consumed by a PMML model
      * @param optMsgVersion the version of the message to be consumed. By default Some(-1)
      * @return the result as a JSON String of object ApiResult where ApiResult.statusCode
      * indicates success or failure of operation: 0 for success, Non-zero for failure. The Value of
      * ApiResult.statusDescription and ApiResult.resultData indicate the nature of the error in case of failure
     */
  override def AddModel( modelType: ModelType.ModelType
                           , input: String
                           , optUserid: Option[String] = None
                           , optModelName: Option[String] = None
                           , optVersion: Option[String] = None
                           , optMsgConsumed: Option[String] = None
                           , optMsgVersion: Option[String] = Some("-1") ): String  = {
        val modelResult : String = modelType match {
            case ModelType.KPMML => {
                AddKPMMLModel(input, optUserid)
            }
            case ModelType.JAVA | ModelType.SCALA => {
                val result : String = optModelName.fold(throw new RuntimeException("Model name should be provided for Java/Scala models"))(name => {
                    AddModelFromSource(input, modelType.toString, name, optUserid)
                })
                result
            }
            case ModelType.PMML => {
                val modelName: String = optModelName.orNull
                val version: String = optVersion.orNull
                val msgConsumed: String = optMsgConsumed.orNull
                val msgVer : String = optMsgVersion.getOrElse("-1")
                val result: String = if (modelName != null && version != null && msgConsumed != null) {
                    val res : String = AddPMMLModel(modelName
                                                    , version
                                                    , msgConsumed
                                                    , msgVer
                                                    , input
                                                    , optUserid)
                    res
                } else {
                    val inputRep: String = if (input != null && input.size > 200) input.substring(0, 199)
                                            else if (input != null) input
                                            else "no model text"
                    val apiResult = new ApiResult(ErrorCodeConstants.Failure
                                                , "AddModel"
                                                , null
                                                , s"One or more PMML arguments have not been specified... modelName = $modelName, version = $version, input = $inputRep error = ${ErrorCodeConstants.Add_Model_Failed}")
                    apiResult.toString
                }
                result
            }

            case ModelType.BINARY =>
                new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, s"BINARY model type NOT SUPPORTED YET ...${ErrorCodeConstants.Add_Model_Failed}").toString

            case _ => {
                    val apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, s"Unknown model type ${modelType.toString} error = ${ErrorCodeConstants.Add_Model_Failed}")
                    apiResult.toString
            }
        }
        modelResult
    }


    /**
     * Add a PMML model to the metadata.
     *
     * PMML models are evaluated, not compiled. To create the model definition, an instance of the evaluator
     * is obtained from the pmml-evaluator component and the ModelDef is constructed and added to the store.
     * @see com.ligadata.MetadataAPI.JpmmlSupport for more information
     *
     * @param modelName the namespace.name of the model to be injested.
     * @param version the version as string in the form "MMMMMM.mmmmmmmm.nnnnnn" (3 nodes .. could be fewer chars per node)
     * @param msgConsumed the namespace.name of the message that this model is to consume.  NOTE: the
     *                    fields used in the pmml model and the fields in the message must match.  If
     *                    the message does not supply all input fields in the model, there should be a default
     *                    specified for those not filled in that mining variable.
     * @param msgVersion the version of the message that this PMML model will consume
     * @param pmmlText the actual PMML (xml) that is submitted by the client.
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return json string result
     */
  private def AddPMMLModel(  modelName : String
                            , version : String
                            , msgConsumed : String
                            , msgVersion : String
                            , pmmlText : String
                            , userid : Option[String]
                            ): String = {
    try {
        val buffer : StringBuilder = new StringBuilder
        val modelNameNodes : Array[String] = modelName.split('.')
        val modelNm : String = modelNameNodes.last
        modelNameNodes.take(modelNameNodes.size - 1).addString(buffer,".")
        val modelNmSpace : String = buffer.toString
        buffer.clear
        val msgNameNodes : Array[String] = msgConsumed.split('.')
        val msgName : String = msgNameNodes.last
        msgNameNodes.take(msgNameNodes.size - 1).addString(buffer,".")
        val msgNamespace : String = buffer.toString
        val jpmmlSupport : JpmmlSupport = new JpmmlSupport(mdMgr
                                                        , modelNmSpace
                                                        , modelNm
                                                        , version
                                                        , msgNamespace
                                                        , msgName
                                                        , msgVersion
                                                        , pmmlText)
        val recompile : Boolean = false
        val modDef : ModelDef = jpmmlSupport.CreateModel(recompile)

        // ModelDef may be null if the model evaluation failed
        val latestVersion : Option[ModelDef] = if (modDef == null) None else GetLatestModel(modDef)
        val isValid: Boolean = if (latestVersion.isDefined) IsValidVersion(latestVersion.get, modDef) else true

        if (isValid && modDef != null) {
            logAuditRec(userid, Some(AuditConstants.WRITE), AuditConstants.INSERTOBJECT, pmmlText, AuditConstants.SUCCESS, "", modDef.FullNameWithVer)
            // save the jar file first
            UploadJarsToDB(modDef)
            val apiResult = AddModel(modDef, userid)
            logger.debug("Model is added..")
            var objectsAdded = new Array[BaseElemDef](0)
            objectsAdded = objectsAdded :+ modDef
            val operations = for (op <- objectsAdded) yield "Add"
            logger.debug("Notify engine via zookeeper")
            NotifyEngine(objectsAdded, operations)
            apiResult
        } else {
            val reasonForFailure: String = if (modDef != null) {
                ErrorCodeConstants.Add_Model_Failed_Higher_Version_Required
            } else {
                ErrorCodeConstants.Add_Model_Failed
            }
            val modDefName: String = if (modDef != null) modDef.FullName else "(pmml compile failed)"
            val modDefVer: String = if (modDef != null) MdMgr.Pad0s2Version(modDef.Version) else MdMgr.UnknownVersion
            var apiResult = new ApiResult(ErrorCodeConstants.Failure
                , "AddModel"
                , null
                , s"$reasonForFailure : $modDefName.$modDefVer)")
            apiResult.toString()
        }
    } catch {
        case e: ModelCompilationFailedException => {
            logger.debug("", e)
            val apiResult = new ApiResult(ErrorCodeConstants.Failure
                                        , "AddModel"
                                        , null
                                        , s"Error : ${e.toString} + ${ErrorCodeConstants.Add_Model_Failed}")
            apiResult.toString()
        }
        case e: AlreadyExistsException => {
            logger.debug("", e)
            val apiResult = new ApiResult(ErrorCodeConstants.Failure
                                        , "AddModel"
                                        , null
                                        , s"Error : ${e.toString} + ${ErrorCodeConstants.Add_Model_Failed}")
            apiResult.toString()
        }
        case e: Exception => {
            logger.debug("", e)
            val apiResult = new ApiResult(ErrorCodeConstants.Failure
                                        , "AddModel"
                                        , null
                                        , s"Error : ${e.toString} + ${ErrorCodeConstants.Add_Model_Failed}")
            apiResult.toString()
        }
    }
  }

    /**
     * Add Kamanja PMML Model (format XML).  Kamanja Pmml models obtain their name and version from the header in the Pmml file.
     * @param pmmlText
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return json string result
     */
  private def AddKPMMLModel(pmmlText: String, userid: Option[String]): String = {
    try {
      var compProxy = new CompilerProxy
      //compProxy.setLoggerLevel(Level.TRACE)
      var (classStr, modDef) = compProxy.compilePmml(pmmlText)

      // ModelDef may be null if there were pmml compiler errors... act accordingly.  If modelDef present,
      // make sure the version of the model is greater than any of previous models with same FullName
      val latestVersion = if (modDef == null) None else GetLatestModel(modDef)
      val isValid: Boolean = if (latestVersion != None) IsValidVersion(latestVersion.get, modDef) else true

      if (isValid && modDef != null) {
        logAuditRec(userid, Some(AuditConstants.WRITE), AuditConstants.INSERTOBJECT, pmmlText, AuditConstants.SUCCESS, "", modDef.FullNameWithVer)
        // save the jar file first
        UploadJarsToDB(modDef)
        val apiResult = AddModel(modDef, userid)
        logger.debug("Model is added..")
        var objectsAdded = new Array[BaseElemDef](0)
        objectsAdded = objectsAdded :+ modDef
        val operations = for (op <- objectsAdded) yield "Add"
        logger.debug("Notify engine via zookeeper")
        NotifyEngine(objectsAdded, operations)
        apiResult
      } else {
        val reasonForFailure: String = if (modDef != null) ErrorCodeConstants.Add_Model_Failed_Higher_Version_Required else ErrorCodeConstants.Add_Model_Failed
        val modDefName: String = if (modDef != null) modDef.FullName else "(kpmml compile failed)"
        val modDefVer: String = if (modDef != null) MdMgr.Pad0s2Version(modDef.Version) else MdMgr.UnknownVersion
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, reasonForFailure + ":" + modDefName + "." + modDefVer)
        apiResult.toString()
      }
    } catch {
      case e: ModelCompilationFailedException => {
        logger.debug("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Model_Failed)
        apiResult.toString()
      }
      case e: AlreadyExistsException => {
        
        logger.debug("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Model_Failed)
        apiResult.toString()
      }
      case e: Exception => {
        
        logger.debug("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Model_Failed)
        apiResult.toString()
      }
    }
  }

    /**
     * Recompile the supplied model. Optionally the message definition is supplied that was just built.
     *
     * @param mod the model definition that possibly needs to be reconstructed.
     * @param userid the user id that has invoked this command
     * @param optMsgDef the MessageDef constructed, assuming it was a message def. If a container def has been rebuilt,
     *               this field will have a value of None.  This is only meaningful at this point when the model to
     *               be rebuilt is a PMML model.
     * @return the result string reflecting what happened with this operation.
     */
    def RecompileModel(mod: ModelDef, userid : Option[String], optMsgDef : Option[MessageDef]): String = {
        try {
            /** FIXME: This should really handle BINARY models too.  When we start supporting them, we cannot recompile
              * it but we can send notifications to consoles or some thing like this to help identify the need for
              * a replacement to the prior binary model.  This needs to be discussed and documented how we are going to
              * do this.
              * FIXME: Actually an update to a message that supports BINARY models needs to be detected up front and
              * a warning and rejection of the message update made. Perhaps adding a "force" flag to get the message
              * to compile despite this obstacle is warranted.
              */
            val isJpmml : Boolean = mod.modelRepresentation == ModelRepresentation.PMML
            val msgDef : MessageDef = optMsgDef.orNull
            val modDef: ModelDef = if (! isJpmml) {

                val compProxy = new CompilerProxy
                //compProxy.setLoggerLevel(Level.TRACE)

                // Recompile the model based upon its model type.Models can be either PMML or Custom Sourced.  See which one we are dealing with
                // here.
                if (mod.objectFormat == ObjFormatType.fXML) {
                    val pmmlText = mod.ObjectDefinition
                    val (classStrTemp, modDefTemp) = compProxy.compilePmml(pmmlText, true)
                    modDefTemp
                } else {
                    val saveModelParms = parse(mod.ObjectDefinition).values.asInstanceOf[Map[String, Any]]
                    val custModDef: ModelDef = compProxy.recompileModelFromSource(
                        saveModelParms.getOrElse(ModelCompilationConstants.SOURCECODE, "").asInstanceOf[String],
                        saveModelParms.getOrElse(ModelCompilationConstants.PHYSICALNAME, "").asInstanceOf[String],
                        saveModelParms.getOrElse(ModelCompilationConstants.DEPENDENCIES, List[String]()).asInstanceOf[List[String]],
                        saveModelParms.getOrElse(ModelCompilationConstants.TYPES_DEPENDENCIES, List[String]()).asInstanceOf[List[String]],
                        ObjFormatType.asString(mod.objectFormat))
                    custModDef
                }
            } else {
                /** the msgConsumed is namespace.name.version  ... drop the version so as to compare the "FullName" */
                val buffer : StringBuilder = new StringBuilder
                val modMsgNameParts : Array[String] = if (mod.msgConsumed != null) mod.msgConsumed.split('.') else Array[String]()
                val modMsgFullName : String = modMsgNameParts.dropRight(1).addString(buffer,".").toString.toLowerCase
                val reasonable : Boolean = (modMsgFullName == msgDef.FullName)
                if (reasonable) {
                    val msgName: String = msgDef.Name
                    val msgNamespace: String = msgDef.NameSpace
                    val msgVersion: String = MdMgr.ConvertLongVersionToString(msgDef.Version)
                    val modelNmSpace : String = mod.NameSpace
                    val modelName : String = mod.Name
                    val modelVersion : String = MdMgr.ConvertLongVersionToString(mod.Version)
                    val jpmmlSupport: JpmmlSupport = new JpmmlSupport(mdMgr
                                                                    , modelNmSpace
                                                                    , modelName
                                                                    , modelVersion
                                                                    , msgNamespace
                                                                    , msgName
                                                                    , msgVersion
                                                                    , mod.jpmmlText)
                    val recompile : Boolean = true
                    val model : ModelDef = jpmmlSupport.CreateModel(recompile)
                    model
                } else {
                    /** this means that the dependencies are incorrect.. message is not the PMML message of interest */
                    logger.error(s"The message names for model ${mod.FullName} and the message just built (${msgDef.FullName}) don't match up. It suggests model dependencies and/or the model type are messed up.")
                    null
                }
            }

            val latestVersion = if (modDef == null) None else GetLatestModel(modDef)
            val isValid: Boolean = (modDef != null)
            if (isValid) {
                val rmResult : String = RemoveModel(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver, None)
                UploadJarsToDB(modDef)
                val addResult : String = AddModel(modDef,userid)
                var objectsUpdated = new Array[BaseElemDef](0)
                var operations = new Array[String](0)
                objectsUpdated = objectsUpdated :+ latestVersion.get
                operations = operations :+ "Remove"
                objectsUpdated = objectsUpdated :+ modDef
                operations = operations :+ "Add"
                NotifyEngine(objectsUpdated, operations)
                s"\nRecompileModel results for ${mod.NameSpace}.${mod.Name}.${mod.Version}\n$rmResult$addResult"
            } else {
                val reasonForFailure: String = ErrorCodeConstants.Model_ReCompilation_Failed
                val modDefName: String = if (mod != null) mod.FullName else "(compilation failed)"
                val modDefVer: String = if (mod != null) MdMgr.Pad0s2Version(mod.Version) else MdMgr.UnknownVersion
                var apiResult = new ApiResult(ErrorCodeConstants.Failure, "\nRecompileModel", null, reasonForFailure + ":" + modDefName + "." + modDefVer)
                apiResult.toString()
            }
        } catch {
            case e: ModelCompilationFailedException => {
                logger.debug("", e)
                var apiResult = new ApiResult(ErrorCodeConstants.Failure, "\nRecompileModel", null, "Error in producing scala file or Jar file.." + ErrorCodeConstants.Add_Model_Failed)
                apiResult.toString()
            }
            case e: AlreadyExistsException => {
                
                logger.debug("", e)
                var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RecompileModel", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Model_Failed)
                apiResult.toString()
            }
            case e: Exception => {
                
                logger.debug("", e)
                var apiResult = new ApiResult(ErrorCodeConstants.Failure, "RecompileModel", null, "Error :" + e.toString() + ErrorCodeConstants.Add_Model_Failed)
                apiResult.toString()
            }
        }
    }

    /**
     * Update the model with new source of type modelType.
     *
     * Except for the modelType and the input, all fields are marked optional. Note, however, that for some of the
     * model types all arguments should have meaningful values.
     *
     * @see AddModel for semantics.
     *
     * Note that the message and message version (as seen in AddModel) are not used.  Should a message change that is being
     * used by one of the PMML models, it will be automatically be updated immediately when the message compilation and
     * metadata update has completed for it.
     *
     * Currently only the most recent model cataloged with the name noted in the source file can be "updated".  It is not
     * possible to have a number of models with the same name differing only by version and be able to update one of them
     * explicitly.  This is a feature that is to be implemented.
     *
     * If both the model and the message are changing, consider using AddModel to create a new PMML model and then remove the older
     * version if appropriate.
     *
     * @param modelType the type of the model submission (any {SCALA,JAVA,PMML,KPMML,BINARY}
     * @param input the text element to be added dependent upon the modelType specified.
     * @param optUserid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method.
     * @param optModelName the namespace.name of the PMML model to be added to the Kamanja metadata (Note: KPMML models extract this from
     *                     the pmml source file header and for this reason is not required for the KPMML model types).
     * @param optVersion the model version to be used to describe this PMML model (for KPMML types this value is obtained from the source file)
     * @param optVersionBeingUpdated not used .. reserved for future release where explicit modelnamespace.modelname.modelversion
     *                               can be updated (not just the latest version)
     * @return  result string indicating success or failure of operation
     */
    override def UpdateModel(modelType: ModelType.ModelType
                            , input: String
                            , optUserid: Option[String] = None
                            , optModelName: Option[String] = None
                            , optVersion: Option[String] = None
                            , optVersionBeingUpdated : Option[String] = None): String = {
        /**
         * FIXME: The current strategy is that only the most recent version can be updated.
         * FIXME: This is not a satisfactory condition. It may be desirable to have 10 models all with
         * FIXME: the same name but differing only in their version numbers. If someone wants to tune
         * FIXME: #6 of the 10, that number six is not the latest.  It is just a unique model.
         *
         * For this reason, the version of the model that is to be changed should be supplied here and all of the
         * associated handler functions that service update for the various model types should be amended to
         * consider which model it is that is to be updated exactly.  The removal of the model being replaced
         * must be properly handled to remove the one with the version supplied.
         */

        val modelResult: String = modelType match {
            case ModelType.KPMML => {
                val result: String = UpdateKPMMLModel(modelType, input, optUserid, optModelName, optVersion)
                result
            }
            case ModelType.JAVA | ModelType.SCALA => {
                val result: String = UpdateCustomModel(modelType, input, optUserid, optModelName, optVersion)
                result
            }
            case ModelType.PMML => {
                val result : String = UpdatePMMLModel(modelType, input, optUserid, optModelName, optVersion, optVersionBeingUpdated)
                result
            }
            case ModelType.BINARY =>
                new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, s"BINARY model type NOT SUPPORTED YET ...${ErrorCodeConstants.Add_Model_Failed}").toString
            case _ => {
                val apiResult = new ApiResult(ErrorCodeConstants.Failure, "AddModel", null, s"Unknown model type ${modelType.toString} error = ${ErrorCodeConstants.Add_Model_Failed}")
                apiResult.toString
            }
        }
        modelResult

    }

    /**
     * Update a PMML model with the supplied inputs.  The input is presumed to be a new version of a PMML model that
     * is currently cataloged.  The user id should be supplied for any installation that is using the security or audit
     * plugins. The model namespace.name and its new version are supplied.  The message ingested by the current version
     * is used by the for the update.
     *
     * @param modelType the type of the model... PMML in this case
     * @param input the new source to ingest for the model being updated/replaced
     * @param optUserid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @param optModelName the name of the model to be ingested (only relevant for PMML ingestion)
     * @param optModelVersion the version number of the model to be updated (only relevant for PMML ingestion)
     * @param optVersionBeingUpdated not used... reserved
     * @return result string indicating success or failure of operation
     */
    private def UpdatePMMLModel(modelType: ModelType.ModelType
                                  , input: String
                                  , optUserid: Option[String] = None
                                  , optModelName: Option[String] = None
                                  , optModelVersion: Option[String] = None
                                  , optVersionBeingUpdated : Option[String] ): String = {

        val modelName: String = optModelName.orNull
        val version: String = optModelVersion.getOrElse("-1")
        val result: String = if (modelName != null && version != null) {
            try {
                val buffer: StringBuilder = new StringBuilder
                val modelNameNodes: Array[String] = modelName.split('.')
                val modelNm: String = modelNameNodes.last
                modelNameNodes.take(modelNameNodes.size - 1).addString(buffer, ".")
                val modelNmSpace: String = buffer.toString

                val currentVer : Long = -1
                val onlyActive : Boolean = false  /** allow active or inactive models to be updated */
                val optCurrent : Option[ModelDef] = mdMgr.Model(modelNmSpace, modelNm, currentVer, onlyActive)
                val currentModel : ModelDef = optCurrent.orNull
                val currentMsg : String = if (currentModel != null) currentModel.msgConsumed else null
                val (currMsgNmSp, currMsgNm, currMsgVer) : (String,String,String) = MdMgr.SplitFullNameWithVersion(currentMsg)

                val jpmmlSupport: JpmmlSupport = new JpmmlSupport(mdMgr
                    , modelNmSpace
                    , modelNm
                    , version
                    , currMsgNmSp
                    , currMsgNm
                    , currMsgVer
                    , input)

                val modDef: ModelDef = jpmmlSupport.UpdateModel

                /**
                 * FIXME: The current strategy is that only the most recent version can be updated.
                 * FIXME: This is not a satisfactory condition. It may be desirable to have 10 models all with
                 * FIXME: the same name but differing only in their version numbers. If someone wants to tune
                 * FIXME: #6 of the 10, that number six is not the latest.  It is just a unique model.
                 *
                 * For this reason, the version of the model that is to be changed should be supplied here and in the
                 * more generic interface implementation that calls here.
                 */

                //def Model(nameSpace: String, name: String, ver: Long, onlyActive: Boolean): Option[ModelDef]
                val tentativeVersionBeingUpdated : String = optVersionBeingUpdated.orNull
                val versionBeingUpdated : String = if (tentativeVersionBeingUpdated != null) tentativeVersionBeingUpdated else "-1"
                val versionLong : Long = MdMgr.ConvertVersionToLong(version)
                val optVersionUpdated : Option[ModelDef] = MdMgr.GetMdMgr.Model(modelNmSpace, modelNm, versionLong, onlyActive)
                val versionUpdated : ModelDef = optVersionUpdated.orNull

                // ModelDef may be null if the model evaluation failed
                // old .... val latestVersion: Option[ModelDef] = if (modDef == null) None else GetLatestModel(modDef) was compared
                // with modeDef in IsValidVersion
                //val isValid: Boolean = if (latestVersion.isDefined) IsValidVersion(latestVersion.get, modDef) else true
                val isValid: Boolean = if (optVersionUpdated.isDefined) IsValidVersion(versionUpdated, modDef) else true

                if (isValid && modDef != null) {
                    logAuditRec(optUserid, Some(AuditConstants.WRITE), AuditConstants.INSERTOBJECT, input, AuditConstants.SUCCESS, "", modDef.FullNameWithVer)

                    /**
                     * FIXME: Considering the design goal of NON-STOP cluster model management, it seems that the window
                     * FIXME: for something to go wrong is too likely with this current approach.  The old model is being
                     * FIXME: deleted before the engine is notified.  Should the engine ask for metadata on that model
                     * FIXME: after the model being updated is removed but before the new version has been added, there
                     * FIXME: is likelihood that unpredictable behavior that would be difficult to diagnose could occur.
                     *
                     * FIXME: Furthermore, who is to say that the user doesn't want the model to be updated all right, but
                     * FIXME: but that they are not sure that they want the old version to be removed.  In other words,
                     * FIXME: "the model is to be updated" means "add the modified version of the model, and atomically
                     * FIXME: swap the old active version (deactivate it) and the new version (activate it)?
                     *
                     * FIXME: We need to think it through... what the semantics of the Update is.  In fact we might want
                     * FIXME: to deprecate it altogether.  There should be just Add model, Activate model, Deactivate model,
                     * FIXME: Swap Models (activate and deactivate same model/different versions atomically), and Remove
                     * FIXME: model. Removes would fail if they are active; they need to be deactivated before removal.
                     *
                     * FIXME: The design goals are to not stop the cluster and to not miss an incoming message. The windows
                     * FIXME: of opportunity for calamity are measured by how long it takes to swap inactive/active.  Everything
                     * FIXME: else is "offline" as it were.
                     *
                     */
                    val rmModelResult : String = if( versionUpdated != null ){
                        RemoveModel(versionUpdated.NameSpace, versionUpdated.Name, versionUpdated.Version, None)
                    } else {
                        ""
                    }
                    logger.info("Begin uploading dependent Jars, please wait...")
                    UploadJarsToDB(modDef)
                    logger.info("uploading dependent Jars complete")

                    val addResult = AddModel(modDef, optUserid)
                    logger.debug("Model is added..")
                    var objectsAdded = new Array[BaseElemDef](0)
                    objectsAdded = objectsAdded :+ modDef
                    val operations = for (op <- objectsAdded) yield "Add"
                    logger.debug("Notify engine via zookeeper")
                    NotifyEngine(objectsAdded, operations)
                    s"UpdateModel version $version of $modelNmSpace.$modelNm results:\n$rmModelResult\n$addResult"
                } else {
                    val reasonForFailure: String = if (modDef != null) {
                        ErrorCodeConstants.Update_Model_Failed_Invalid_Version
                    } else {
                        ErrorCodeConstants.Update_Model_Failed
                    }
                    val modDefName: String = if (modDef != null) modDef.FullName else "(pmml compile failed)"
                    val modDefVer: String = if (modDef != null) MdMgr.Pad0s2Version(modDef.Version) else MdMgr.UnknownVersion
                    var apiResult = new ApiResult(ErrorCodeConstants.Failure
                        , "AddModel"
                        , null
                        , s"$reasonForFailure : $modDefName.$modDefVer)")
                    apiResult.toString()
                }
            } catch {
                case e: ModelCompilationFailedException => {
                    logger.debug("", e)
                    val apiResult = new ApiResult(ErrorCodeConstants.Failure
                        , s"UpdateModel(type = PMML)"
                        , null
                        , s"Error : ${e.toString} + ${ErrorCodeConstants.Update_Model_Failed}")
                    apiResult.toString()
                }
                case e: AlreadyExistsException => {
                    
                    logger.debug("", e)
                    val apiResult = new ApiResult(ErrorCodeConstants.Failure
                        , s"UpdateModel(type = PMML)"
                        , null
                        , s"Error : ${e.toString} + ${ErrorCodeConstants.Update_Model_Failed}")
                    apiResult.toString()
                }
                case e: Exception => {
                    
                    logger.debug("", e)
                    val apiResult = new ApiResult(ErrorCodeConstants.Failure
                        , s"UpdateModel(type = PMML)"
                        , null
                        , s"Error : ${e.toString} + ${ErrorCodeConstants.Update_Model_Failed}")
                    apiResult.toString()
                }
            }
        } else {
            val apiResult = new ApiResult(ErrorCodeConstants.Failure
                , s"UpdateModel(type = PMML)"
                , null
                , s"The model name and new version was not supplied for this PMML model : name=$modelName version=$version\nOptionally one should consider supplying the exact version of the model being updated, especially important when you are maintaining multiple versions with the same model name and tweaking versions of same for your 'a/b/c...' score comparisons.")
            apiResult.toString()

        }
        result
    }

    /**
     * Update the java or scala model with new source.
     *
     * @param modelType the type of the model... JAVA | SCALA in this case
     * @param input the source of the model to ingest
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @param modelName the name of the model to be ingested (PMML)
     *                  or the model's config for java and scala
     * @param version the version number of the model to be updated (only relevant for PMML ingestion)
     * @return result string indicating success or failure of operation
     */
    private def UpdateCustomModel(modelType: ModelType.ModelType
                                  , input: String
                                  , userid: Option[String] = None
                                  , modelName: Option[String] = None
                                  , version: Option[String] = None): String = {
        val sourceLang : String = modelType.toString /** to get here it is either 'java' or 'scala' */
        try {
            val compProxy = new CompilerProxy
            compProxy.setSessionUserId(userid)
            val modelNm : String = modelName.orNull
            val modDef : ModelDef =  compProxy.compileModelFromSource(input, modelNm, sourceLang)

            /**
             * FIXME: The current strategy is that only the most recent version can be updated.
             * FIXME: This is not a satisfactory condition. It may be desirable to have 10 models all with
             * FIXME: the same name but differing only in their version numbers. If someone wants to tune
             * FIXME: #6 of the 10, that number six is not the latest.  It is just a unique model.
             *
             * For this reason, the version of the model that is to be changed should be supplied here and in the
             * more generic interface implementation that calls here.
             */

            val latestVersion = if (modDef == null) None else GetLatestModel(modDef)
            val isValid: Boolean = if (latestVersion != None) IsValidVersion(latestVersion.get, modDef) else true

            if (isValid && modDef != null) {
                logAuditRec(userid, Some(AuditConstants.WRITE), AuditConstants.UPDATEOBJECT, input, AuditConstants.SUCCESS, "", modDef.FullNameWithVer)
                val key = MdMgr.MkFullNameWithVersion(modDef.nameSpace, modDef.name, modDef.ver)
                if( latestVersion != None ){
                    RemoveModel(latestVersion.get.nameSpace, latestVersion.get.name, latestVersion.get.ver, None)
                }
                logger.info("Begin uploading dependent Jars, please wait...")
                UploadJarsToDB(modDef)
                logger.info("Finished uploading dependent Jars.")
                val apiResult = AddModel(modDef, userid)
                var objectsUpdated = new Array[BaseElemDef](0)
                var operations = new Array[String](0)
                if( latestVersion != None ) {
                    objectsUpdated = objectsUpdated :+ latestVersion.get
                    operations = operations :+ "Remove"
                }
                objectsUpdated = objectsUpdated :+ modDef
                operations = operations :+ "Add"
                NotifyEngine(objectsUpdated, operations)
                apiResult
            } else {
                val reasonForFailure: String = if (modDef != null) ErrorCodeConstants.Add_Model_Failed_Higher_Version_Required else ErrorCodeConstants.Add_Model_Failed
                val modDefName: String = if (modDef != null) modDef.FullName else "(source compile failed)"
                val modDefVer: String = if (modDef != null) MdMgr.Pad0s2Version(modDef.Version) else MdMgr.UnknownVersion
                var apiResult = new ApiResult(ErrorCodeConstants.Failure, "UpdateModel", null, reasonForFailure + ":" + modDefName + "." + modDefVer)
                apiResult.toString()
            }
        } catch {
            case e: ModelCompilationFailedException => {
                logger.debug("", e)
                var apiResult = new ApiResult(ErrorCodeConstants.Failure, s"${'"'}UpdateModel(type = $sourceLang)${'"'}", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Model_Failed)
                apiResult.toString()
            }
            case e: AlreadyExistsException => {
                
                logger.debug("", e)
                var apiResult = new ApiResult(ErrorCodeConstants.Failure, s"${'"'}UpdateModel(type = $sourceLang)${'"'}", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Model_Failed)
                apiResult.toString()
            }
            case e: Exception => {
                
                logger.debug("", e)
                var apiResult = new ApiResult(ErrorCodeConstants.Failure, s"${'"'}UpdateModel(type = $sourceLang)${'"'}", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Model_Failed)
                apiResult.toString()
            }
        }
    }

    /**
     * UpdateModel - Update a Kamanja Pmml model
     *
     * Current semantics are that the source supplied in pmmlText is compiled and a new model is reproduced. The Kamanja
     * PMML version is specified in the KPMML source itself in the header's Version attribute. The version of the updated
     * model must be > the most recent cataloged one that is being updated. With this strategy ONLY the most recent
     * version can be updated.
     *
     * @param modelType the type of the model submission... PMML in this case
     * @param pmmlText the text element to be added dependent upon the modelType specified.
     * @param optUserid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method.
     * @param optModelName the model's namespace.name (ignored in this implementation of the UpdatePmmlModel... only used in PMML updates)
     * @param optVersion the model's version (ignored in this implementation of the UpdatePmmlModel... only used in PMML updates)
     * @return  result string indicating success or failure of operation
     */
    private def UpdateKPMMLModel(modelType: ModelType.ModelType
                             , pmmlText: String
                             , optUserid: Option[String] = None
                             , optModelName: Option[String] = None
                             , optVersion: Option[String] = None ): String = {
        try {
            var compProxy = new CompilerProxy
            //compProxy.setLoggerLevel(Level.TRACE)
            var (classStr, modDef) = compProxy.compilePmml(pmmlText)
            val optLatestVersion = if (modDef == null) None else GetLatestModel(modDef)
            val latestVersion : ModelDef = optLatestVersion.orNull

            /**
             * FIXME: The current strategy is that only the most recent version can be updated.
             * FIXME: This is not a satisfactory condition. It may be desirable to have 10 PMML models all with
             * FIXME: the same name but differing only in their version numbers. If someone wants to tune
             * FIXME: #6 of the 10, that number six is not the latest.  It is just a unique model.
             *
             * For this reason, the version of the model that is to be changed should be supplied here and in the
             * more generic interface implementation that calls here.
             */

            val isValid: Boolean = (modDef != null && latestVersion != null && latestVersion.Version <  modDef.Version)

            if (isValid && modDef != null) {
                logAuditRec(optUserid, Some(AuditConstants.WRITE), AuditConstants.UPDATEOBJECT, pmmlText, AuditConstants.SUCCESS, "", modDef.FullNameWithVer)
                val key = MdMgr.MkFullNameWithVersion(modDef.nameSpace, modDef.name, modDef.ver)

                // when a version number changes, latestVersion  has different namespace making it unique
                // latest version may not be found in the cache. So need to remove it
                if( latestVersion != None ) {
                    RemoveModel(latestVersion.nameSpace, latestVersion.name, latestVersion.ver, None)
                }

                UploadJarsToDB(modDef)
                val result = AddModel(modDef,optUserid)
                var objectsUpdated = new Array[BaseElemDef](0)
                var operations = new Array[String](0)

                if( latestVersion != None ) {
                    objectsUpdated = objectsUpdated :+ latestVersion
                    operations = operations :+ "Remove"
                }

                objectsUpdated = objectsUpdated :+ modDef
                operations = operations :+ "Add"
                NotifyEngine(objectsUpdated, operations)
                result

            } else {
                val reasonForFailure: String = if (modDef != null) ErrorCodeConstants.Update_Model_Failed_Invalid_Version else ErrorCodeConstants.Update_Model_Failed
                val modDefName: String = if (modDef != null) modDef.FullName else "(kpmml compile failed)"
                val modDefVer: String = if (modDef != null) MdMgr.Pad0s2Version(modDef.Version) else MdMgr.UnknownVersion
                var apiResult = new ApiResult(ErrorCodeConstants.Failure, s"UpdateModel(type = KPMML)", null, reasonForFailure + ":" + modDefName + "." + modDefVer)
                apiResult.toString()
            }
        } catch {
            case e: ObjectNotFoundException => {
                logger.debug("", e)
                var apiResult = new ApiResult(ErrorCodeConstants.Failure, s"UpdateModel(type = KPMML)", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Model_Failed)
                apiResult.toString()
            }
            case e: Exception => {
                
                logger.debug("", e)
                var apiResult = new ApiResult(ErrorCodeConstants.Failure, s"UpdateModel(type = KPMML)", null, "Error :" + e.toString() + ErrorCodeConstants.Update_Model_Failed)
                apiResult.toString()
            }
        }
  }

    /**
     * GetDependentModels
     * @param msgNameSpace
     * @param msgName
     * @param msgVer
     * @return
     */
  def GetDependentModels(msgNameSpace: String, msgName: String, msgVer: Long): Array[ModelDef] = {
    MessageAndContainerUtils.GetDependentModels(msgNameSpace,msgName,msgVer)
  }

  def GetContainerDefFromCache(nameSpace: String, name: String, formatType: String, version: String, userid: Option[String]): String = {
    MessageAndContainerUtils.GetContainerDefFromCache(nameSpace,name,formatType,version,userid)
  }
    /**
     * Get all available models (format JSON or XML) as string.
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None
     * @return string representation in specified format.
     */
  def GetAllModelDefs(formatType: String, userid: Option[String] = None): String = {
    try {
      val modDefs = MdMgr.GetMdMgr.Models(true, true)
      modDefs match {
        case None =>
          None
          logger.debug("No Models found ")
          val apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllModelDefs", null, ErrorCodeConstants.Get_All_Models_Failed_Not_Available)
          apiResult.toString()
        case Some(ms) =>
          val msa = ms.toArray
          val apiResult = new ApiResult(ErrorCodeConstants.Success, "GetAllModelDefs", JsonSerializer.SerializeObjectListToJson("Models", msa), ErrorCodeConstants.Get_All_Models_Successful)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        
        logger.debug("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetAllModelDefs", null, "Error :" + e.toString() + ErrorCodeConstants.Get_All_Models_Failed)
        apiResult.toString()
      }
    }
  }

    /**
     * GetAllMessageDefs - get all available messages(format JSON or XML) as a String
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetAllMessageDefs(formatType: String, userid: Option[String] = None): String = {
    MessageAndContainerUtils.GetAllMessageDefs(formatType,userid)
  }

  // All available containers(format JSON or XML) as a String
    /**
     * GetAllContainerDefs
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return result as string
     */
  def GetAllContainerDefs(formatType: String, userid: Option[String] = None): String = {
    MessageAndContainerUtils.GetAllContainerDefs(formatType,userid)
  }

    /**
     * GetAllModelsFromCache
     * @param active
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetAllModelsFromCache(active: Boolean, userid: Option[String] = None): Array[String] = {
    var modelList: Array[String] = new Array[String](0)
    if (userid != None) logAuditRec(userid, Some(AuditConstants.READ), AuditConstants.GETKEYS, AuditConstants.MODEL, AuditConstants.SUCCESS, "", AuditConstants.MODEL)
    try {
      val modDefs = MdMgr.GetMdMgr.Models(active, true)
      modDefs match {
        case None =>
          None
          logger.debug("No Models found ")
          modelList
        case Some(ms) =>
          val msa = ms.toArray
          val modCount = msa.length
          modelList = new Array[String](modCount)
          for (i <- 0 to modCount - 1) {
            modelList(i) = msa(i).FullName + "." + MdMgr.Pad0s2Version(msa(i).Version)
          }
          modelList
      }
    } catch {
      case e: Exception => {
        
        logger.debug("", e)
        throw UnexpectedMetadataAPIException("Failed to fetch all the models:" + e.toString, e)
      }
    }
  }

    /**
     * GetAllMessagesFromCache
     * @param active
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetAllMessagesFromCache(active: Boolean, userid: Option[String] = None): Array[String] = {
    MessageAndContainerUtils.GetAllMessagesFromCache(active,userid)
  }

    /**
     * GetAllContainersFromCache
     * @param active
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetAllContainersFromCache(active: Boolean, userid: Option[String] = None): Array[String] = {
    MessageAndContainerUtils.GetAllContainersFromCache(active,userid)
  }

    /**
     * GetAllFunctionsFromCache
     * @param active
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetAllFunctionsFromCache(active: Boolean, userid: Option[String] = None): Array[String] = {
    FunctionUtils.GetAllFunctionsFromCache(active,userid)
  }

    /**
     * GetAllConceptsFromCache
     * @param active
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetAllConceptsFromCache(active: Boolean, userid: Option[String] = None): Array[String] = {
    ConceptUtils.GetAllConceptsFromCache(active,userid)
  }

    /**
     * GetAllTypesFromCache
     * @param active <description please>
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return <description please>
     */
  def GetAllTypesFromCache(active: Boolean, userid: Option[String] = None): Array[String] = {
    TypeUtils.GetAllTypesFromCache(active,userid)
  }

  // Specific models (format JSON or XML) as an array of strings using modelName(without version) as the key
    /**
     * 
     * @param nameSpace namespace of the object
     * @param objectName name of the desired object, possibly namespace qualified
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetModelDef(nameSpace: String, objectName: String, formatType: String, userid : Option[String]): String = {
    try {
      val modDefs = MdMgr.GetMdMgr.Models(nameSpace, objectName, true, true)
      modDefs match {
        case None =>
          None
          logger.debug("No Models found ")
          val apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetModelDef", null, ErrorCodeConstants.Get_Model_Failed_Not_Available + ":" + nameSpace + "." + objectName)
          apiResult.toString()
        case Some(ms) =>
          val msa = ms.toArray
          val apiResult = new ApiResult(ErrorCodeConstants.Success, "GetModelDef", JsonSerializer.SerializeObjectListToJson("Models", msa), ErrorCodeConstants.Get_Model_Successful)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        
        logger.debug("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetModelDef", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Model_Failed + ":" + nameSpace + "." + objectName)
        apiResult.toString()
      }
    }
  }

    /**
     * Get a specific models (format JSON or XML) as an array of strings using modelName(without version) as the key
     * @param objectName name of the desired object, possibly namespace qualified
     * @param formatType format of the return value, either JSON or XML
     * @return
     */
  def GetModelDef(objectName: String, formatType: String, userid : Option[String] = None): String = {
    GetModelDef(sysNS, objectName, formatType, userid)
  }

    /**
     * Get a specific model (format JSON or XML) as a String using modelName(with version) as the key
     * @param nameSpace namespace of the object
     * @param name
     * @param formatType format of the return value, either JSON or XML
     * @param version  Version of the object
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetModelDefFromCache(nameSpace: String, name: String, formatType: String, version: String, userid: Option[String] = None): String = {
    val dispkey = nameSpace + "." + name + "." + MdMgr.Pad0s2Version(version.toLong)
    if (userid != None) logAuditRec(userid, Some(AuditConstants.WRITE), AuditConstants.GETOBJECT, AuditConstants.MODEL, AuditConstants.SUCCESS, "", dispkey)
    try {
      var key = nameSpace + "." + name + "." + version.toLong
      val o = MdMgr.GetMdMgr.Model(nameSpace.toLowerCase, name.toLowerCase, version.toLong, true)
      o match {
        case None =>
          None
          logger.debug("model not found => " + dispkey)
          val apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetModelDefFromCache", null, ErrorCodeConstants.Get_Model_From_Cache_Failed_Not_Active + ":" + dispkey)
          apiResult.toString()
        case Some(m) =>
          logger.debug("model found => " + m.asInstanceOf[ModelDef].FullName + "." + MdMgr.Pad0s2Version(m.asInstanceOf[ModelDef].Version))
          val apiResult = new ApiResult(ErrorCodeConstants.Success, "GetModelDefFromCache", JsonSerializer.SerializeObjectToJson(m), ErrorCodeConstants.Get_Model_From_Cache_Successful + ":" + dispkey)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        
        logger.debug("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetModelDefFromCache", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Model_From_Cache_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  }

  // Specific models (format JSON or XML) as an array of strings using modelName(without version) as the key
    /**
     * 
     * @param nameSpace namespace of the object
     * @param objectName name of the desired object, possibly namespace qualified
     * @param formatType format of the return value, either JSON or XML
     * @param version  Version of the object
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetModelDef(nameSpace: String, objectName: String, formatType: String, version: String, userid: Option[String]): String = {
    logAuditRec(userid
        , Some(AuditConstants.READ)
        , AuditConstants.GETOBJECT
        , AuditConstants.MODEL
        , AuditConstants.SUCCESS
        , ""
        , nameSpace + "." + objectName + "." + version)
    GetModelDefFromCache(nameSpace, objectName, formatType, version, None)
  }

    /**
     * Get the specific message (format JSON or XML) as a String using messageName(with version) as the key
     * @param nameSpace namespace of the object
     * @param name
     * @param formatType format of the return value, either JSON or XML
     * @param version  Version of the object
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetMessageDefFromCache(nameSpace: String, name: String, formatType: String, version: String, userid: Option[String] = None): String = {
    val dispkey = nameSpace + "." + name + "." + MdMgr.Pad0s2Version(version.toLong)
    var key = nameSpace + "." + name + "." + version.toLong
    if (userid != None) logAuditRec(userid, Some(AuditConstants.GETOBJECT), AuditConstants.GETOBJECT, AuditConstants.MESSAGE, AuditConstants.SUCCESS, "", dispkey)
    try {
      val o = MdMgr.GetMdMgr.Message(nameSpace.toLowerCase, name.toLowerCase, version.toLong, true)
      o match {
        case None =>
          None
          logger.debug("message not found => " + dispkey)
          val apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetMessageDefFromCache", null, ErrorCodeConstants.Get_Message_From_Cache_Failed + ":" + dispkey)
          apiResult.toString()
        case Some(m) =>
          logger.debug("message found => " + m.asInstanceOf[MessageDef].FullName + "." + MdMgr.Pad0s2Version(m.asInstanceOf[MessageDef].Version))
          val apiResult = new ApiResult(ErrorCodeConstants.Success, "GetMessageDefFromCache", JsonSerializer.SerializeObjectToJson(m), ErrorCodeConstants.Get_Message_From_Cache_Successful)
          apiResult.toString()
      }
    } catch {
      case e: Exception => {
        
        logger.debug("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetMessageDefFromCache", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Message_From_Cache_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  }

    /**
     * Return Specific messageDef object using messageName(with version) as the key
     * @param nameSpace namespace of the object
     * @param name
     * @param formatType format of the return value, either JSON or XML
     * @param version  Version of the object
     * @return
     */
  @throws(classOf[ObjectNotFoundException])
  def GetMessageDefInstanceFromCache(nameSpace: String, name: String, formatType: String, version: String): MessageDef = {
    MessageAndContainerUtils.GetMessageDefInstanceFromCache(nameSpace,name,formatType,version)
  }

    /**
     * Check whether model already exists in metadata manager. Ideally,
     * we should never add the model into metadata manager more than once
     * and there is no need to use this function in main code flow
     * This is just a utility function being used during these initial phases
     * @param modDef the model def to be tested
     * @return
     */
  def DoesModelAlreadyExist(modDef: ModelDef): Boolean = {
    try {
      var key = modDef.nameSpace + "." + modDef.name + "." + modDef.ver
      val dispkey = modDef.nameSpace + "." + modDef.name + "." + MdMgr.Pad0s2Version(modDef.ver)
      val o = MdMgr.GetMdMgr.Model(modDef.nameSpace.toLowerCase,
        modDef.name.toLowerCase,
        modDef.ver,
        false)
      o match {
        case None =>
          None
          logger.debug("model not in the cache => " + dispkey)
          return false;
        case Some(m) =>
          logger.debug("model found => " + m.asInstanceOf[ModelDef].FullName + "." + MdMgr.Pad0s2Version(m.asInstanceOf[ModelDef].ver))
          return true
      }
    } catch {
      case e: Exception => {
        
        logger.debug("", e)
        throw UnexpectedMetadataAPIException(e.getMessage(), e)
      }
    }
  }

    /**
     * Get the latest model for a given FullName
     * @param modDef
     * @return
     */
  def GetLatestModel(modDef: ModelDef): Option[ModelDef] = {
    try {
      var key = modDef.nameSpace + "." + modDef.name + "." + modDef.ver
      val dispkey = modDef.nameSpace + "." + modDef.name + "." + MdMgr.Pad0s2Version(modDef.ver)
      val o = MdMgr.GetMdMgr.Models(modDef.nameSpace.toLowerCase,
        modDef.name.toLowerCase, false, true)
      o match {
        case None =>
          None
          logger.debug("model not in the cache => " + dispkey)
          None
        case Some(m) =>
          if (m.size > 0) {
            logger.debug("model found => " + m.head.asInstanceOf[ModelDef].FullName + "." + MdMgr.Pad0s2Version(m.head.asInstanceOf[ModelDef].ver))
            Some(m.head.asInstanceOf[ModelDef])
          } else
            None
      }
    } catch {
      case e: Exception => {
        
        logger.debug("", e)
        throw UnexpectedMetadataAPIException(e.getMessage(), e)
      }
    }
  }

  //
    /**
     * Get the latest cataloged models from the supplied set
     * @param modelSet
     * @return
     */
  def GetLatestModelFromModels(modelSet: Set[ModelDef]): ModelDef = {
    var model: ModelDef = null
    var verList: List[Long] = List[Long]()
    var modelmap: scala.collection.mutable.Map[Long, ModelDef] = scala.collection.mutable.Map()
    try {
      modelSet.foreach(m => {
        modelmap.put(m.Version, m)
        verList = m.Version :: verList
      })
      model = modelmap.getOrElse(verList.max, null)
    } catch {
      case e: Exception => {
        logger.debug("", e)
        throw new Exception("Error in traversing Model set", e)
      }
    }
    model
  }

    /**
     * GetLatestFunction
     * @param fDef
     * @return
     */
  def GetLatestFunction(fDef: FunctionDef): Option[FunctionDef] = {
    FunctionUtils.GetLatestFunction(fDef)
  }

  // Get the latest message for a given FullName
    /**
     * 
     * @param msgDef
     * @return
     */
  def GetLatestMessage(msgDef: MessageDef): Option[MessageDef] = {
    MessageAndContainerUtils.GetLatestMessage(msgDef)
  }

    /**
     * Get the latest container for a given FullName
     * @param contDef
     * @return
     */
  def GetLatestContainer(contDef: ContainerDef): Option[ContainerDef] = {
    MessageAndContainerUtils.GetLatestContainer(contDef)
  }

    /**
     * IsValidVersion
     * @param oldObj
     * @param newObj
     * @return
     */
    def IsValidVersion(oldObj: BaseElemDef, newObj: BaseElemDef): Boolean = {
      if (newObj.ver > oldObj.ver) {
        return true
      } else {
        return false
      }
    }


    /**
     * Check whether message already exists in metadata manager. Ideally,
     * we should never add the message into metadata manager more than once
     * and there is no need to use this function in main code flow
     * This is just a utility function being during these initial phases
     * @param msgDef
     * @return
     */
    def DoesMessageAlreadyExist(msgDef: MessageDef): Boolean = {
        MessageAndContainerUtils.IsMessageAlreadyExists(msgDef)
    }

    /**
     * Check whether message already exists in metadata manager. Ideally,
     * we should never add the message into metadata manager more than once
     * and there is no need to use this function in main code flow
     * This is just a utility function being during these initial phases
     * @param msgDef
     * @return
     */
    def IsMessageAlreadyExists(msgDef: MessageDef): Boolean = {
      MessageAndContainerUtils.IsMessageAlreadyExists(msgDef)      
    }

    /**
     * DoesContainerAlreadyExist
     * @param contDef
     * @return
     */
    def DoesContainerAlreadyExist(contDef: ContainerDef): Boolean = {
        MessageAndContainerUtils.IsContainerAlreadyExists(contDef)
    }

    /**
     * IsContainerAlreadyExists
     * @param contDef
     * @return
     */
  def IsContainerAlreadyExists(contDef: ContainerDef): Boolean = {
    MessageAndContainerUtils.IsContainerAlreadyExists(contDef)
  }

    /**
     * DoesConceptAlreadyExist
     * @param attrDef
     * @return
     */
    def DoesConceptAlreadyExist(attrDef: BaseAttributeDef): Boolean = {
        ConceptUtils.IsConceptAlreadyExists(attrDef)
    }

    /**
     * IsConceptAlreadyExists
     * @param attrDef
     * @return
     */
    def IsConceptAlreadyExists(attrDef: BaseAttributeDef): Boolean = {
      ConceptUtils.IsConceptAlreadyExists(attrDef)
    }

    /**
     * Get a specific model definition from persistent store
     * @param nameSpace namespace of the object
     * @param objectName name of the desired object, possibly namespace qualified
     * @param formatType format of the return value, either JSON or XML
     * @param version  Version of the object
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetModelDefFromDB(nameSpace: String, objectName: String, formatType: String, version: String, userid: Option[String] = None): String = {
    var key = "ModelDef" + "." + nameSpace + '.' + objectName + "." + version.toLong
    val dispkey = "ModelDef" + "." + nameSpace + '.' + objectName + "." + MdMgr.Pad0s2Version(version.toLong)
    if (userid != None) logAuditRec(userid, Some(AuditConstants.WRITE), AuditConstants.GETOBJECT, AuditConstants.MODEL, AuditConstants.SUCCESS, "", dispkey)
    try {
      var obj = GetObject(key.toLowerCase, "models")
      var apiResult = new ApiResult(ErrorCodeConstants.Success, "GetModelDefFromCache", new String(obj.serializedInfo), ErrorCodeConstants.Get_Model_From_DB_Successful + ":" + dispkey)
      apiResult.toString()
    } catch {
      case e: Exception => {
        
        logger.debug("", e)
        val apiResult = new ApiResult(ErrorCodeConstants.Failure, "GetModelDefFromCache", null, "Error :" + e.toString() + ErrorCodeConstants.Get_Model_From_DB_Failed + ":" + dispkey)
        apiResult.toString()
      }
    }
  }

    /**
     * IsTypeObject
     * @param typeName
     * @return
     */
  private def IsTypeObject(typeName: String): Boolean = {
    typeName match {
      case "scalartypedef" | "arraytypedef" | "arraybuftypedef" | "listtypedef" | "settypedef" | "treesettypedef" | "queuetypedef" | "maptypedef" | "immutablemaptypedef" | "hashmaptypedef" | "tupletypedef" | "structtypedef" | "sortedsettypedef" => {
        return true
      }
      case _ => {
        return false
      }
    }
  }

    /**
     * GetAllKeys
     * @param objectType
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetAllKeys(objectType: String, userid: Option[String] = None): Array[String] = {
    try {
      var keys = scala.collection.mutable.Set[String]()

      // get keys for types "types", "functions", "messages", "containers", "concepts", "models"
      val reqTypes = Array("types", "functions", "messages", "containers", "concepts", "models")
      val processedContainersSet = Set[String]()

      reqTypes.foreach(typ => {
        val storeInfo = tableStoreMap(typ)

        if (processedContainersSet(storeInfo._1) == false) {
          processedContainersSet += storeInfo._1
          storeInfo._2.getKeys(storeInfo._1, { (key: Key) =>
            {
              val strKey = key.bucketKey.mkString(".")
              val i = strKey.indexOf(".")
              val objType = strKey.substring(0, i)
              val typeName = strKey.substring(i + 1)
              objectType match {
                case "TypeDef" => {
                  if (IsTypeObject(objType)) {
                    keys.add(typeName)
                  }
                  if (userid != None) logAuditRec(userid, Some(AuditConstants.READ), AuditConstants.GETKEYS, AuditConstants.TYPE, AuditConstants.SUCCESS, "", AuditConstants.TYPE)
                }
                case "FunctionDef" => {
                  if (objType == "functiondef") {
                    keys.add(typeName)
                  }
                  if (userid != None) logAuditRec(userid, Some(AuditConstants.READ), AuditConstants.GETKEYS, AuditConstants.FUNCTION, AuditConstants.SUCCESS, "", AuditConstants.FUNCTION)
                }
                case "MessageDef" => {
                  if (objType == "messagedef") {
                    keys.add(typeName)
                  }
                  if (userid != None) logAuditRec(userid, Some(AuditConstants.READ), AuditConstants.GETKEYS, AuditConstants.MESSAGE, AuditConstants.SUCCESS, "", AuditConstants.MESSAGE)
                }
                case "ContainerDef" => {
                  if (objType == "containerdef") {
                    keys.add(typeName)
                  }
                  if (userid != None) logAuditRec(userid, Some(AuditConstants.READ), AuditConstants.GETKEYS, AuditConstants.CONTAINER, AuditConstants.SUCCESS, "", AuditConstants.CONTAINER)
                }
                case "Concept" => {
                  if (objType == "attributedef") {
                    keys.add(typeName)
                  }
                  if (userid != None) logAuditRec(userid, Some(AuditConstants.READ), AuditConstants.GETKEYS, AuditConstants.CONCEPT, AuditConstants.SUCCESS, "", AuditConstants.CONCEPT)
                }
                case "ModelDef" => {
                  if (objType == "modeldef") {
                    keys.add(typeName)
                  }
                  if (userid != None) logAuditRec(userid, Some(AuditConstants.READ), AuditConstants.GETKEYS, AuditConstants.MODEL, AuditConstants.SUCCESS, "", AuditConstants.MODEL)
                }
                case _ => {
                  logger.error("Unknown object type " + objectType + " in GetAllKeys function")
                  throw InternalErrorException("Unknown object type " + objectType + " in GetAllKeys function", null)
                }
              }
            }
          })
        }
      })

      keys.toArray
    } catch {
      case e: Exception => {
        
        logger.debug("", e)
        throw InternalErrorException("Failed to get keys from persistent store", e)
      }
    }
  }

    /**
     * LoadAllConfigObjectsIntoCache
     * @return
     */
  def LoadAllConfigObjectsIntoCache: Boolean = {
    try {
      var processed: Long = 0L
      val storeInfo = tableStoreMap("config_objects")
      storeInfo._2.get(storeInfo._1, { (k: Key, v: Value) =>
        {
          val strKey = k.bucketKey.mkString(".")
          val i = strKey.indexOf(".")
          val objType = strKey.substring(0, i)
          val typeName = strKey.substring(i + 1)
          processed += 1
          objType match {
            case "nodeinfo" => {
              val ni = serializer.DeserializeObjectFromByteArray(v.serializedInfo).asInstanceOf[NodeInfo]
              MdMgr.GetMdMgr.AddNode(ni)
            }
            case "adapterinfo" => {
              val ai = serializer.DeserializeObjectFromByteArray(v.serializedInfo).asInstanceOf[AdapterInfo]
              MdMgr.GetMdMgr.AddAdapter(ai)
            }
            case "clusterinfo" => {
              val ci = serializer.DeserializeObjectFromByteArray(v.serializedInfo).asInstanceOf[ClusterInfo]
              MdMgr.GetMdMgr.AddCluster(ci)
            }
            case "clustercfginfo" => {
              val ci = serializer.DeserializeObjectFromByteArray(v.serializedInfo).asInstanceOf[ClusterCfgInfo]
              MdMgr.GetMdMgr.AddClusterCfg(ci)
            }
            case "userproperties" => {
              val up = serializer.DeserializeObjectFromByteArray(v.serializedInfo).asInstanceOf[UserPropertiesInfo]
              MdMgr.GetMdMgr.AddUserProperty(up)
            }
            case _ => {
              throw InternalErrorException("LoadAllConfigObjectsIntoCache: Unknown objectType " + objType, null)
            }
          }
        }
      })

      if (processed == 0) {
        logger.debug("No config objects available in the Database")
        return false
      }

      return true
    } catch {
      case e: Exception => {
        
        logger.debug("", e)
        return false
      }
    }
  }

    /**
     * LoadAllModelConfigsIntoChache
     */
  private def LoadAllModelConfigsIntoCache: Unit = {
    val maxTranId = GetTranId
    currentTranLevel = maxTranId
    logger.debug("Max Transaction Id => " + maxTranId)

    var processed: Long = 0L
    val storeInfo = tableStoreMap("model_config_objects")
    storeInfo._2.get(storeInfo._1, { (k: Key, v: Value) =>
      {
        processed += 1
        val conf = serializer.DeserializeObjectFromByteArray(v.serializedInfo).asInstanceOf[Map[String, List[String]]]
        MdMgr.GetMdMgr.AddModelConfig(k.bucketKey.mkString("."), conf)
      }
    })

    if (processed == 0) {
      logger.debug("No model config objects available in the Database")
      return
    }
    MdMgr.GetMdMgr.DumpModelConfigs
  }

    /**
     * LoadAllObjectsIntoCache
     */
  def LoadAllObjectsIntoCache {
    try {
      val configAvailable = LoadAllConfigObjectsIntoCache
      if (configAvailable) {
        RefreshApiConfigForGivenNode(metadataAPIConfig.getProperty("NODE_ID"))
      } else {
        logger.debug("Assuming bootstrap... No config objects in persistent store")
      }

      // Load All the Model Configs here... 
      LoadAllModelConfigsIntoCache
      //LoadAllUserPopertiesIntoChache
      startup = true
      val maxTranId = currentTranLevel
      var objectsChanged = new Array[BaseElemDef](0)
      var operations = new Array[String](0)

      val reqTypes = Array("types", "functions", "messages", "containers", "concepts", "models")
      val processedContainersSet = Set[String]()
      var processed: Long = 0L

      reqTypes.foreach(typ => {
        val storeInfo = tableStoreMap(typ)
        if (processedContainersSet(storeInfo._1) == false) {
          processedContainersSet += storeInfo._1
          storeInfo._2.get(storeInfo._1, { (k: Key, v: Value) =>
            {
              val mObj = serializer.DeserializeObjectFromByteArray(v.serializedInfo).asInstanceOf[BaseElemDef]
              if (mObj != null) {
                if (mObj.tranId <= maxTranId) {
                  AddObjectToCache(mObj, MdMgr.GetMdMgr)
                  DownloadJarFromDB(mObj)
                } else {
                  if (mObj.isInstanceOf[FunctionDef]) {
                    // BUGBUG:: Not notifying functions at this moment. This may cause inconsistance between different instances of the metadata.
                  } else {
                    logger.debug("The transaction id of the object => " + mObj.tranId)
                    AddObjectToCache(mObj, MdMgr.GetMdMgr)
                    DownloadJarFromDB(mObj)
                    logger.error("Transaction is incomplete with the object " + k.bucketKey.mkString(",") + ",we may not have notified engine, attempt to do it now...")
                    objectsChanged = objectsChanged :+ mObj
                    if (mObj.IsActive) {
                      operations = for (op <- objectsChanged) yield "Add"
                    } else {
                      operations = for (op <- objectsChanged) yield "Remove"
                    }
                  }
                }
              } else {
                throw InternalErrorException("serializer.Deserialize returned a null object", null)
              }
            }
            processed += 1
          })
        }
      })

      if (processed == 0) {
        logger.debug("No metadata objects available in the Database")
        return
      }

      if (objectsChanged.length > 0) {
        NotifyEngine(objectsChanged, operations)
      }
      startup = false
    } catch {
      case e: Exception => {
        
        logger.debug("", e)
      }
    }
  }

  def LoadMessageIntoCache(key: String) {
    try {
      logger.debug("Fetch the object " + key + " from database ")
      val obj = GetObject(key.toLowerCase, "messages")
      logger.debug("Deserialize the object " + key)
      val msg = serializer.DeserializeObjectFromByteArray(obj.serializedInfo)
      logger.debug("Get the jar from database ")
      val msgDef = msg.asInstanceOf[MessageDef]
      DownloadJarFromDB(msgDef)
      logger.debug("Add the object " + key + " to the cache ")
      AddObjectToCache(msgDef, MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
        logger.error("Failed to load message into cache " + key, e)
      }
    }
  }

    /**
     * LoadTypeIntoCache
     * @param key
     */
  def LoadTypeIntoCache(key: String) {
    try {
      logger.debug("Fetch the object " + key + " from database ")
      val obj = GetObject(key.toLowerCase, "types")
      logger.debug("Deserialize the object " + key)
      val typ = serializer.DeserializeObjectFromByteArray(obj.serializedInfo)
      if (typ != null) {
        logger.debug("Add the object " + key + " to the cache ")
        AddObjectToCache(typ, MdMgr.GetMdMgr)
      }
    } catch {
      case e: Exception => {
        
        logger.warn("Unable to load the object " + key + " into cache ", e)
      }
    }
  }

    /**
     * LoadModelIntoCache
     * @param key
     */
  def LoadModelIntoCache(key: String) {
    try {
      logger.debug("Fetch the object " + key + " from database ")
      val obj = GetObject(key.toLowerCase, "models")
      logger.debug("Deserialize the object " + key)
      val model = serializer.DeserializeObjectFromByteArray(obj.serializedInfo)
      logger.debug("Get the jar from database ")
      val modDef = model.asInstanceOf[ModelDef]
      DownloadJarFromDB(modDef)
      logger.debug("Add the object " + key + " to the cache ")
      AddObjectToCache(modDef, MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
        
        logger.debug("", e)
      }
    }
  }

    /**
     * LoadContainerIntoCache
     * @param key
     */
  def LoadContainerIntoCache(key: String) {
    try {
      val obj = GetObject(key.toLowerCase, "containers")
      val cont = serializer.DeserializeObjectFromByteArray(obj.serializedInfo)
      logger.debug("Get the jar from database ")
      val contDef = cont.asInstanceOf[ContainerDef]
      DownloadJarFromDB(contDef)
      AddObjectToCache(contDef, MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
        
        logger.debug("", e)
      }
    }
  }

    /**
     * LoadAttributeIntoCache
     * @param key
     */
  def LoadAttributeIntoCache(key: String) {
    try {
      val obj = GetObject(key.toLowerCase, "concepts")
      val cont = serializer.DeserializeObjectFromByteArray(obj.serializedInfo)
      AddObjectToCache(cont.asInstanceOf[AttributeDef], MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
        
        logger.debug("", e)
      }
    }
  }

    /**
     * updateThisKey
     * @param zkMessage
     * @param tranId
     */
  private def updateThisKey(zkMessage: ZooKeeperNotification, tranId: Long) {

    var key: String = (zkMessage.ObjectType + "." + zkMessage.NameSpace + "." + zkMessage.Name + "." + zkMessage.Version.toLong).toLowerCase
    val dispkey = (zkMessage.ObjectType + "." + zkMessage.NameSpace + "." + zkMessage.Name + "." + MdMgr.Pad0s2Version(zkMessage.Version.toLong)).toLowerCase

    zkMessage.ObjectType match {
      case "ConfigDef" => {
        zkMessage.Operation match {
          case "Add" => {
            val inConfig = "{\"" + zkMessage.Name + "\":" + zkMessage.ConfigContnent.get + "}"
            AddConfigObjToCache(tranId, zkMessage.NameSpace + "." + zkMessage.Name, parse(inConfig).values.asInstanceOf[Map[String, List[String]]], MdMgr.GetMdMgr)
          }
          case _ => { logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..") }
        }
      }
      case "ModelDef" => {
        zkMessage.Operation match {
          case "Add" => {
            LoadModelIntoCache(key)
          }
          case "Remove" | "Activate" | "Deactivate" => {
            try {
              MdMgr.GetMdMgr.ModifyModel(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, zkMessage.Operation)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + dispkey + " nolonger exists in metadata : It may have been removed already", e)
              }
            }
          }
          case _ => { logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..") }
        }
      }
      case "MessageDef" => {
        zkMessage.Operation match {
          case "Add" => {
            LoadMessageIntoCache(key)
          }
          case "Remove" => {
            try {
              RemoveMessage(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, None, false)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + dispkey + " nolonger exists in metadata : It may have been removed already", e)
              }
            }
          }
          case "Activate" | "Deactivate" => {
            try {
              MdMgr.GetMdMgr.ModifyMessage(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, zkMessage.Operation)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + dispkey + " nolonger exists in metadata : It may have been removed already", e)
              }
            }
          }
          case _ => { logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..") }
        }
      }
      case "ContainerDef" => {
        zkMessage.Operation match {
          case "Add" => {
            LoadContainerIntoCache(key)
          }
          case "Remove" => {
            try {
              RemoveContainer(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, None, false)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + dispkey + " nolonger exists in metadata : It may have been removed already", e)
              }
            }
          }
          case "Activate" | "Deactivate" => {
            try {
              MdMgr.GetMdMgr.ModifyContainer(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, zkMessage.Operation)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + dispkey + " nolonger exists in metadata : It may have been removed already", e)
              }
            }
          }
          case _ => { logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..") }
        }
      }
      case "FunctionDef" => {
        zkMessage.Operation match {
          case "Add" => {
            FunctionUtils.LoadFunctionIntoCache(key)
          }
          case "Remove" | "Activate" | "Deactivate" => {
            try {
              MdMgr.GetMdMgr.ModifyFunction(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, zkMessage.Operation)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + dispkey + " nolonger exists in metadata : It may have been removed already", e)
              }
            }
          }
          case _ => { logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..") }
        }
      }
      case "AttributeDef" => {
        zkMessage.Operation match {
          case "Add" => {
            LoadAttributeIntoCache(key)
          }
          case "Remove" | "Activate" | "Deactivate" => {
            try {
              MdMgr.GetMdMgr.ModifyAttribute(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, zkMessage.Operation)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + dispkey + " nolonger exists in metadata : It may have been removed already", e)
              }
            }
          }
          case _ => { logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..") }
        }
      }
      case "JarDef" => {
        zkMessage.Operation match {
          case "Add" => {
            DownloadJarFromDB(MdMgr.GetMdMgr.MakeJarDef(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version))
          }
          case _ => { logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..") }
        }
      }
      case "ScalarTypeDef" | "ArrayTypeDef" | "ArrayBufTypeDef" | "ListTypeDef" | "MappedMsgTypeDef" | "SetTypeDef" | "TreeSetTypeDef" | "QueueTypeDef" | "MapTypeDef" | "ImmutableMapTypeDef" | "HashMapTypeDef" | "TupleTypeDef" | "StructTypeDef" | "SortedSetTypeDef" => {
        zkMessage.Operation match {
          case "Add" => {
            LoadTypeIntoCache(key)
          }
          case "Remove" | "Activate" | "Deactivate" => {
            try {
              logger.debug("Remove the type " + dispkey + " from cache ")
              MdMgr.GetMdMgr.ModifyType(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, zkMessage.Operation)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + dispkey + " nolonger exists in metadata : It may have been removed already", e)
              }
            }
          }
          case _ => { logger.error("Unknown Operation " + zkMessage.Operation + " in zookeeper notification, notification is not processed ..") }
        }
      }
      case "OutputMsgDef" => {
        zkMessage.Operation match {
          case "Add" => {
            LoadOutputMsgIntoCache(key)
          }
          case "Remove" | "Activate" | "Deactivate" => {
            try {
              MdMgr.GetMdMgr.ModifyOutputMsg(zkMessage.NameSpace, zkMessage.Name, zkMessage.Version.toLong, zkMessage.Operation)
            } catch {
              case e: ObjectNolongerExistsException => {
                logger.error("The object " + key + " nolonger exists in metadata : It may have been removed already", e)
              }
            }
          }
        }
      }
      case _ => { logger.error("Unknown objectType " + zkMessage.ObjectType + " in zookeeper notification, notification is not processed ..") }
    }
  }

    /**
     * LoadOutputMsgIntoCache
     * @param key
     */
  def LoadOutputMsgIntoCache(key: String) {
    try {
      logger.debug("Fetch the object " + key + " from database ")
      val obj = GetObject(key.toLowerCase, "outputmsgs")
      logger.debug("Deserialize the object " + key)
      val outputMsg = serializer.DeserializeObjectFromByteArray(obj.serializedInfo)
      val outputMsgDef = outputMsg.asInstanceOf[OutputMsgDef]
      logger.debug("Add the output msg def object " + key + " to the cache ")
      AddObjectToCache(outputMsgDef, MdMgr.GetMdMgr)
    } catch {
      case e: Exception => {
        
        logger.debug("", e)
      }
    }
  }

    /**
     * UpdateMdMgr from zookeeper
     * @param zkTransaction
     */
  def UpdateMdMgr(zkTransaction: ZooKeeperTransaction): Unit = {
    var key: String = null
    var dispkey: String = null

    // If we already processed this transaction, currTranLevel will be at least at the level of this notify.
    if (zkTransaction.transactionId.getOrElse("0").toLong <= currentTranLevel) return

    try {
      zkTransaction.Notifications.foreach(zkMessage => {
        key = (zkMessage.Operation + "." + zkMessage.NameSpace + "." + zkMessage.Name + "." + zkMessage.Version).toLowerCase
        dispkey = (zkMessage.Operation + "." + zkMessage.NameSpace + "." + zkMessage.Name + "." + MdMgr.Pad0s2Version(zkMessage.Version.toLong)).toLowerCase
        if (!cacheOfOwnChanges.contains(key)) {
          // Proceed with update.
          updateThisKey(zkMessage, zkTransaction.transactionId.getOrElse("0").toLong)
        } else {
          // Ignore the update, remove the element from set.
          cacheOfOwnChanges.remove(key)
        }
      })
    } catch {
      case e: AlreadyExistsException => {
        
        logger.warn("Failed to load the object(" + dispkey + ") into cache", e)
      }
      case e: Exception => {
        
        logger.warn("Failed to load the object(" + dispkey + ") into cache", e)
      }
    }
  }

   /**
    * Get a the most recent mesage def (format JSON or XML) as a String
    * @param objectName the name of the message possibly namespace qualified (is simple name, "system" namespace is substituted)
    * @param formatType format of the return value, either JSON or XML
    * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
    *               method. If Security and/or Audit are configured, this value must be a value other than None.
    * @return
    */
  def GetMessageDef(objectName: String, formatType: String, userid: Option[String] = None): String = {
      MessageAndContainerUtils.GetMessageDef(objectName,formatType,userid)
  }

    /**
     * Get a specific message (format JSON or XML) as a String using messageName(with version) as the key
     * @param objectName Name of the MessageDef, possibly namespace qualified.
     * @param version  Version of the MessageDef
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return the result as a JSON String of object ApiResult where ApiResult.resultData contains
     *         the MessageDef either as a JSON or XML string depending on the parameter formatType
     */
    def GetMessageDef(objectName: String, version: String, formatType: String, userid: Option[String]): String = {
      MessageAndContainerUtils.GetMessageDef(objectName,version,formatType,userid)
    }

    /**
     * Get a specific message (format JSON or XML) as a String using messageName(with version) as the key
     * @param nameSpace namespace of the object
     * @param objectName Name of the MessageDef
     * @param version  Version of the MessageDef
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return the result as a JSON String of object ApiResult where ApiResult.resultData contains
     *         the MessageDef either as a JSON or XML string depending on the parameter formatType
     */
    def GetMessageDef(nameSpace: String, objectName: String, formatType: String, version: String, userid: Option[String]): String = {
      MessageAndContainerUtils.GetMessageDef(nameSpace,objectName,formatType,version,userid)
    }
    /**
     * Get a specific container (format JSON or XML) as a String using containerName(without version) as the key
     * @param objectName Name of the ContainerDef, possibly namespace qualified. When no namespace, "system" substituted
     * @param formatType
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return the result as a JSON String of object ApiResult where ApiResult.resultData contains
     *         the ContainerDef either as a JSON or XML string depending on the parameter formatType
     */
  def GetContainerDef(objectName: String, formatType: String, userid: Option[String] = None): String = {
      MessageAndContainerUtils.GetContainerDef(objectName,formatType,userid)
  }

    /**
     * Get a specific container (format JSON or XML) as a String using containerName(with version) as the key
     * @param nameSpace namespace of the object
     * @param objectName Name of the ContainerDef
     * @param formatType format of the return value, either JSON or XML format of the return value, either JSON or XML
     * @param version  Version of the ContainerDef
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return the result as a JSON String of object ApiResult where ApiResult.resultData contains
     *         the ContainerDef either as a JSON or XML string depending on the parameter formatType
     */
  def GetContainerDef(nameSpace: String
                      , objectName: String
                      , formatType: String
                      , version: String
                      , userid: Option[String]): String = {
      MessageAndContainerUtils.GetContainerDef(nameSpace,objectName,formatType,version,userid)
  }

    /**
     * Get a specific container (format JSON or XML) as a String using containerName(without version) as the key
     * @param objectName Name of the ContainerDef, possibly namespace qualified. When no namespace, "system" substituted
     * @param version  Version of the object
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return the result as a JSON String of object ApiResult where ApiResult.resultData contains
     *         the ContainerDef either as a JSON or XML string depending on the parameter formatType
     */
  def GetContainerDef(objectName: String, version: String, formatType: String, userid: Option[String]): String = {
    MessageAndContainerUtils.GetContainerDef(objectName,version,formatType,userid)
  }

    /**
    * AddFunctions
    * @param functionsText an input String of functions in a format defined by the next parameter formatType
    * @param formatType format of functionsText ( JSON or XML)
    * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
    *               method. If Security and/or Audit are configured, this value must be a value other than None.
    * @return the result as a JSON String of object ApiResult where ApiResult.statusCode
    *         indicates success or failure of operation: 0 for success, Non-zero for failure. The Value of
    *         ApiResult.statusDescription and ApiResult.resultData indicate the nature of the error in case of failure
    *
    *         Example:
    *         {{{
    *            val sampleFunctionStr = """
    *           {
    *  "NameSpace" : "pmml",
    *  "Name" : "my_min",
    *  "PhysicalName" : "com.ligadata.pmml.udfs.Udfs.Min",
    *  "ReturnTypeNameSpace" : "system",
    *  "ReturnTypeName" : "double",
    *  "Arguments" : [ {
    *  "ArgName" : "expr1",
    *  "ArgTypeNameSpace" : "system",
    *  "ArgTypeName" : "int"
    *  }, {
    *  "ArgName" : "expr2",
    *  "ArgTypeNameSpace" : "system",
    *  "ArgTypeName" : "double"
    *  } ],
    *  "Version" : 1,
    *  "JarName" : null,
    *  "DependantJars" : [ "basetypes_2.10-0.1.0.jar", "metadata_2.10-1.0.jar" ]
    *  }
    *"""
    *    var apiResult = MetadataAPIImpl.AddFunction(sampleFunctionStr,"JSON")
    *    var result = MetadataAPIImpl.getApiResult(apiResult)
    *    println("Result as Json String => \n" + result._2)
    *}}}
    */
  def AddFunctions(functionsText:String, formatType:String, userid: Option[String] = None): String = {
    FunctionUtils.AddFunctions(functionsText,formatType,userid)
  }

    /**
    * UpdateFunctions
    * @param functionsText an input String of functions in a format defined by the next parameter formatType
    * @param formatType format of functionsText ( JSON or XML)
    * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
    *               method. If Security and/or Audit are configured, this value must be a value other than None.
    * @return the result as a JSON String of object ApiResult where ApiResult.statusCode
    *         indicates success or failure of operation: 0 for success, Non-zero for failure. The Value of
    *         ApiResult.statusDescription and ApiResult.resultData indicate the nature of the error in case of failure
    *
    *         Example:
    *         {{{
    *            val sampleFunctionStr = """
    *           {
    *  "NameSpace" : "pmml",
    *  "Name" : "my_min",
    *  "PhysicalName" : "com.ligadata.pmml.udfs.Udfs.Min",
    *  "ReturnTypeNameSpace" : "system",
    *  "ReturnTypeName" : "double",
    *  "Arguments" : [ {
    *  "ArgName" : "expr1",
    *  "ArgTypeNameSpace" : "system",
    *  "ArgTypeName" : "int"
    *  }, {
    *  "ArgName" : "expr2",
    *  "ArgTypeNameSpace" : "system",
    *  "ArgTypeName" : "double"
    *  } ],
    *  "Version" : 1,
    *  "JarName" : null,
    *  "DependantJars" : [ "basetypes_2.10-0.1.0.jar", "metadata_2.10-1.0.jar" ]
    *  }
    *"""
    *    var apiResult = MetadataAPIImpl.UpdateFunction(sampleFunctionStr,"JSON")
    *    var result = MetadataAPIImpl.getApiResult(apiResult)
    *    println("Result as Json String => \n" + result._2)         * }}}
    *
    */
  def UpdateFunctions(functionsText:String, formatType:String, userid: Option[String] = None): String = {
    FunctionUtils.UpdateFunctions(functionsText,formatType,userid)
  }

    /**
     *   def RemoveFunction(nameSpace:String, functionName:String, version:Long, userid: Option[String] = None): String = {

     * @param nameSpace the function's namespace
     * @param functionName name of the function
     * @param version  Version of the object
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return the result as a JSON String of object ApiResult where ApiResult.statusCode
     *         indicates success or failure of operation: 0 for success, Non-zero for failure. The Value of
     *         ApiResult.statusDescription and ApiResult.resultData indicate the nature of the error in case of failure
     *
     *         Example:
     *         {{{
     *          val apiResult = MetadataAPIImpl.RemoveFunction(MdMgr.sysNS,"my_min",100)
     *          val (statusCode,resultData) = MetadataAPIImpl.getApiResult(apiResult)
     *          println("Result as Json String => \n" + resultData)
     *         }}}
     *
     */
  def RemoveFunction(nameSpace:String, functionName:String, version:Long, userid: Option[String] = None): String = {
    FunctionUtils.RemoveFunction(nameSpace,functionName,version,userid)
  }

    /**
     * GetAllFunctionDefs
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return the function count and the result as a JSON String of object ApiResult where ApiResult.resultData contains
     *         the FunctionDef(s) either as a JSON or XML string depending on the parameter formatType as a Tuple2[Int,String]
     */
  def GetAllFunctionDefs(formatType: String, userid: Option[String] = None): (Int, String) = {
    FunctionUtils.GetAllFunctionDefs(formatType,userid)
  }

    /**
     * GetFunctionDef
     * @param objectName Name of the FunctionDef
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return the result as a JSON String of object ApiResult where ApiResult.resultData contains
     *         the FunctionDef(s) either as a JSON or XML string depending on the parameter formatType
     */
  def GetFunctionDef(objectName:String,formatType: String, userid: Option[String] = None) : String = {
    FunctionUtils.GetFunctionDef(objectName,formatType,userid)
  }

    /**
     * GetFunctionDef
     * @param nameSpace namespace of the object 
     * @param objectName name of the desired object, possibly namespace qualified
     * @param formatType format of the return value, either JSON or XML
     * @param version  Version of the object
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetFunctionDef(nameSpace: String, objectName: String, formatType: String, version: String, userid: Option[String]): String = {
    FunctionUtils.GetFunctionDef(nameSpace, objectName, formatType, version, userid)
  }

    /**
     * GetFunctionDef
     * @param objectName Name of the FunctionDef
     * @param version  Version of the FunctionDef
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return the result as a JSON String of object ApiResult where ApiResult.resultData contains
     *         the FunctionDef either as a JSON or XML string depending on the parameter formatType
     */
  def GetFunctionDef( objectName: String, version: String, formatType: String, userid: Option[String]) : String = {
    val nameSpace = MdMgr.sysNS /** FIXME: This should be removed and the object name parsed for the namespace and name */
    FunctionUtils.GetFunctionDef(nameSpace, objectName, formatType, version, userid)
  }

    /**
     * Get all available concepts as a String
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return the result as a JSON String of object ApiResult where ApiResult.resultData contains
     *         the Concept(s) either as a JSON or XML string depending on the parameter formatType
     */
  def GetAllConcepts(formatType: String, userid: Option[String] = None): String = {
    ConceptUtils.GetAllConcepts(formatType,userid)
  }

    /**
     * Get a single concept as a string using name and version as the key
     * @param nameSpace namespace of the object
     * @param objectName name of the desired object, possibly namespace qualified
     * @param version  Version of the object
     * @param formatType format of the return value, either JSON or XML
     * @return
     */
  def GetConcept(nameSpace: String, objectName: String, version: String, formatType: String, userid: Option[String]): String = {
    ConceptUtils.GetConcept(nameSpace,objectName,version,formatType)
  }

    /**
     * Get a single concept as a string using name and version as the key
     * @param objectName name of the desired object, possibly namespace qualified
     * @param version  Version of the object
     * @param formatType format of the return value, either JSON or XML
     * @return
     */
  def GetConcept(objectName: String, version: String, formatType: String, userid: Option[String]): String = {
    GetConcept(MdMgr.sysNS, objectName, version, formatType, userid)
  }


    /**
     * Get a single concept as a string using name and version as the key
     * @param nameSpace namespace of the object
     * @param objectName name of the desired object, possibly namespace qualified
     * @param formatType format of the return value, either JSON or XML
     * @param version  Version of the object
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetConceptDef(nameSpace: String, objectName: String, formatType: String,
                    version: String, userid: Option[String]): String = {
    ConceptUtils.GetConceptDef(nameSpace, objectName, formatType, version, userid)
  }

    /**
     * Get a list of concept(s) as a string using name
     * @param objectName name of the desired object, possibly namespace qualified
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetConcept(objectName: String, formatType: String, userid: Option[String] = None): String = {
    ConceptUtils.GetConcept(objectName,formatType)
  }

    /**
     * Get all available derived concepts(format JSON or XML) as a String
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetAllDerivedConcepts(formatType: String, userid: Option[String] = None): String = {
    ConceptUtils.GetAllDerivedConcepts(formatType)
  }

  //
    /**
     * Get a derived concept(format JSON or XML) as a string using name(without version) as the key
     * @param objectName name of the desired object, possibly namespace qualified
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetDerivedConcept(objectName: String, formatType: String, userid: Option[String] = None): String = {
    ConceptUtils.GetDerivedConcept(objectName,formatType)
  }

    /**
     * GetDerivedConcept - A derived concept(format JSON or XML) as a string using name and version as the key
     * @param objectName name of the desired object, possibly namespace qualified
     * @param version  Version of the object
     * @param formatType format of the return value, either JSON or XML
     * @return
     */
  def GetDerivedConcept(objectName: String, version: String, formatType: String, userid: Option[String]): String = {
    ConceptUtils.GetDerivedConcept(objectName,version,formatType)
  }

   /**
    * GetAllTypes - All available types(format JSON or XML) as a String
    * @param formatType format of the return value, either JSON or XML
    * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
    *               method. If Security and/or Audit are configured, this value must be a value other than None.
    * @return the result as a JSON String of object ApiResult where ApiResult.resultData contains
    *         the available types as a JSON or XML string depending on the parameter formatType
    */
  def GetAllTypes(formatType: String, userid: Option[String] = None): String = {
    TypeUtils.GetAllTypes(formatType,userid)
  }

    /**
     * GetAllTypesByObjType - All available types(format JSON or XML) as a String
     * @param formatType format of the return value, either JSON or XML
     * @param objType
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetAllTypesByObjType(formatType: String, objType: String, userid: Option[String] = None): String = {
    TypeUtils.GetAllTypesByObjType(formatType,objType)
  }

    /**
     * GetType
     * @param objectName name of the desired object, possibly namespace qualified
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  // Get types for a given name
  def GetType(objectName: String, formatType: String, userid: Option[String] = None): String = {
    TypeUtils.GetType(objectName,formatType)
  }

    /**
     * GetTypeDef
     * @param nameSpace namespace of the object
     * @param objectName name of the desired object, possibly namespace qualified
     * @param formatType format of the return value, either JSON or XML
     * @param version  Version of the object
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetTypeDef(nameSpace: String, objectName: String, formatType: String, version: String, userid: Option[String] = None): String = {
    TypeUtils.GetTypeDef(nameSpace,objectName,formatType,version,userid)
  }

    /**
     * GetType
     * @param nameSpace namespace of the object
     * @param objectName name of the desired object, possibly namespace qualified
     * @param version  Version of the object
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def GetType(nameSpace: String, objectName: String, version: String, formatType: String, userid: Option[String]): Option[BaseTypeDef] = {
    TypeUtils.GetType(nameSpace, objectName, version, formatType, userid)
  }

    /**
     * AddNode
     * @param nodeId a cluster node
     * @param nodePort
     * @param nodeIpAddr
     * @param jarPaths Set of paths where jars are located
     * @param scala_home
     * @param java_home
     * @param classpath
     * @param clusterId
     * @param power
     * @param roles
     * @param description
     * @return
     */
  def AddNode(nodeId: String, nodePort: Int, nodeIpAddr: String,
    jarPaths: List[String], scala_home: String,
    java_home: String, classpath: String,
    clusterId: String, power: Int,
    roles: Array[String], description: String): String = {
    ConfigUtils.AddNode(nodeId, nodePort, nodeIpAddr, jarPaths, scala_home,
        java_home, classpath, clusterId, power, roles, description)
  }

    /**
     * UpdateNode
     * @param nodeId a cluster node
     * @param nodePort
     * @param nodeIpAddr
     * @param jarPaths Set of paths where jars are located
     * @param scala_home
     * @param java_home
     * @param classpath
     * @param clusterId
     * @param power
     * @param roles
     * @param description
     * @return
     */
  def UpdateNode(nodeId: String, nodePort: Int, nodeIpAddr: String,
    jarPaths: List[String], scala_home: String,
    java_home: String, classpath: String,
    clusterId: String, power: Int,
    roles: Array[String], description: String): String = {
    ConfigUtils.AddNode(nodeId, nodePort, nodeIpAddr, jarPaths, scala_home,
      java_home, classpath,
      clusterId, power, roles, description)
  }

    /**
     * RemoveNode
     * @param nodeId a cluster node
     * @return
     */
  def RemoveNode(nodeId: String): String = {
    ConfigUtils.RemoveNode(nodeId)
  }

    /**
     * AddAdapter
     * @param name
     * @param typeString
     * @param dataFormat
     * @param className
     * @param jarName
     * @param dependencyJars
     * @param adapterSpecificCfg
     * @param inputAdapterToVerify
     * @param delimiterString
     * @param associatedMsg
     * @return
     */
  def AddAdapter(name: String, typeString: String, dataFormat: String, className: String,
                 jarName: String, dependencyJars: List[String],
                 adapterSpecificCfg: String, inputAdapterToVerify: String, keyAndValueDelimiter: String, fieldDelimiter: String, valueDelimiter: String, associatedMsg: String, failedEventsAdapter: String): String = {
    ConfigUtils.AddAdapter(name, typeString, dataFormat, className, jarName,
        dependencyJars, adapterSpecificCfg, inputAdapterToVerify, keyAndValueDelimiter, fieldDelimiter, valueDelimiter, associatedMsg, failedEventsAdapter)
  }

    /**
     * RemoveAdapter
     * @param name
     * @param typeString
     * @param dataFormat
     * @param className
     * @param jarName
     * @param dependencyJars
     * @param adapterSpecificCfg
     * @param inputAdapterToVerify
     * @param delimiterString
     * @param associatedMsg
     * @return
     */
  def UpdateAdapter(name: String, typeString: String, dataFormat: String, className: String,
                    jarName: String, dependencyJars: List[String],
                    adapterSpecificCfg: String, inputAdapterToVerify: String, keyAndValueDelimiter: String, fieldDelimiter: String, valueDelimiter: String, associatedMsg: String, failedEventsAdapter: String): String = {
    ConfigUtils.AddAdapter(name, typeString, dataFormat, className, jarName, dependencyJars, adapterSpecificCfg, inputAdapterToVerify, keyAndValueDelimiter, fieldDelimiter, valueDelimiter, associatedMsg, failedEventsAdapter)
  }

    /**
     * RemoveAdapter
     * @param name
     * @return
     */
  def RemoveAdapter(name: String): String = {
    ConfigUtils.RemoveAdapter(name)
  }

    /**
     * AddCluster
     * @param clusterId
     * @param description
     * @param privileges
     * @return
     */
  def AddCluster(clusterId: String, description: String, privileges: String): String = {
    ConfigUtils.AddCluster(clusterId,description,privileges)
  }

    /**
     * UpdateCluster
     * @param clusterId
     * @param description
     * @param privileges
     * @return
     */
  def UpdateCluster(clusterId: String, description: String, privileges: String): String = {
    ConfigUtils.AddCluster(clusterId, description, privileges)
  }

    /**
     * RemoveCluster
     * @param clusterId
     * @return
     */
  def RemoveCluster(clusterId: String): String = {
    ConfigUtils.RemoveCluster(clusterId)
  }

    /**
     * Add a cluster configuration from the supplied map with the supplied identifer key
     * @param clusterCfgId cluster id to add
     * @param cfgMap the configuration map
     * @param modifiedTime when modified
     * @param createdTime when created
     * @return results string
     */
  def AddClusterCfg(clusterCfgId: String, cfgMap: scala.collection.mutable.HashMap[String, String],
    modifiedTime: Date, createdTime: Date): String = {
    ConfigUtils.AddClusterCfg(clusterCfgId, cfgMap, modifiedTime, createdTime)
  }

    /**
     * Update te configuration for the cluster with the supplied id
     * @param clusterCfgId
     * @param cfgMap
     * @param modifiedTime
     * @param createdTime
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def UpdateClusterCfg(clusterCfgId: String, cfgMap: scala.collection.mutable.HashMap[String, String],
    modifiedTime: Date, createdTime: Date, userid: Option[String] = None): String = {
    ConfigUtils.AddClusterCfg(clusterCfgId, cfgMap, modifiedTime, createdTime)
  }

    /**
     * Remove a cluster configuration with the suppplied id
     *
     * @param clusterCfgId
     * @return results string
     */
  def RemoveClusterCfg(clusterCfgId: String, userid: Option[String] = None): String = {
    ConfigUtils.RemoveClusterCfg(clusterCfgId)
  }

    /**
     * Remove a cluster configuration
     * @param cfgStr
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @param cobjects
     * @return results string
     */
  def RemoveConfig(cfgStr: String, userid: Option[String], cobjects: String): String = {
    ConfigUtils.RemoveConfig(cfgStr,userid,cobjects)
  }

    /**
     * Answer the model compilation dependencies
     * FIXME: Which ones? input or output?
     * @param modelConfigName
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def getModelDependencies(modelConfigName: String, userid: Option[String] = None): List[String] = {
    var config: scala.collection.immutable.Map[String, List[String]] = MdMgr.GetMdMgr.GetModelConfig(modelConfigName)
    config.getOrElse(ModelCompilationConstants.DEPENDENCIES, List[String]())
  }

    /**
     * getModelMessagesContainers
     * @param modelConfigName
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @return
     */
  def getModelMessagesContainers(modelConfigName: String, userid: Option[String] = None): List[String] = {
    MessageAndContainerUtils.getModelMessagesContainers(modelConfigName,userid)
  }

    /**
     * Get the model config keys
     * @return
     */
  def getModelConfigNames(): Array[String] = {
    MdMgr.GetMdMgr.GetModelConfigKeys
  }

  /**
   *
   */
  private var cfgmap: Map[String, Any] = null

    /**
     * Upload a model config.  These are for native models written in Scala or Java
     * @param cfgStr
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @param objectList
     * @param isFromNotify
     * @return
     */
  def UploadModelsConfig(cfgStr: String, userid: Option[String], objectList: String, isFromNotify: Boolean = false): String = {
    ConfigUtils.UploadModelsConfig(cfgStr,userid,objectList,isFromNotify)
  }

    /**
     * Accept a config specification (a JSON str)
     * @param cfgStr the json file to be interpted
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. If Security and/or Audit are configured, this value must be a value other than None.
     * @param objectList note on the objects in the configuration to be logged to audit adapter
     * @return
     */
  def UploadConfig(cfgStr: String, userid: Option[String], objectList: String): String = {
    ConfigUtils.UploadConfig(cfgStr,userid,objectList)
  }

    /**
     * Get a property value
     * @param ci
     * @param key
     * @return
     */
  def getUP(ci: String, key: String): String = {
    ConfigUtils.getUP(ci,key)
  }

    /**
     * Answer nodes as an array.
     * @return
     */
  def getNodeList1: Array[NodeInfo] = { 
    ConfigUtils.getNodeList1
  }
  // All available nodes(format JSON) as a String
    /**
     * Get the nodes as json.
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. The default is None, but if Security and/or Audit are configured, this value is of little practical use.
     *               Supply one.
     * @return
     */
  def GetAllNodes(formatType: String, userid: Option[String] = None): String = {
    ConfigUtils.GetAllNodes(formatType,userid)
  }

    /**
     * All available adapters(format JSON) as a String
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. The default is None, but if Security and/or Audit are configured, this value is of little practical use.
     *               Supply one.
     * @return
     */
  def GetAllAdapters(formatType: String, userid: Option[String] = None): String = {
    ConfigUtils.GetAllAdapters(formatType,userid)
  }

    /**
     * All available clusters(format JSON) as a String
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. The default is None, but if Security and/or Audit are configured, this value is of little practical use.
     *               Supply one.
     * @return
     */
  def GetAllClusters(formatType: String, userid: Option[String] = None): String = {
    ConfigUtils.GetAllClusters(formatType,userid)
  }

  // All available clusterCfgs(format JSON) as a String
    /**
     *
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. The default is None, but if Security and/or Audit are configured, this value is of little practical use.
     *               Supply one.
     * @return
     */
  def GetAllClusterCfgs(formatType: String, userid: Option[String] = None): String = {
    ConfigUtils.GetAllClusterCfgs(formatType,userid)
  }

    /**
     * All available config objects(format JSON) as a String
     * @param formatType format of the return value, either JSON or XML
     * @param userid the identity to be used by the security adapter to ascertain if this user has access permissions for this
     *               method. The default is None, but if Security and/or Audit are configured, this value is of little practical use.
     *               Supply one.
     * @return
     */
  def GetAllCfgObjects(formatType: String, userid: Option[String] = None): String = {
    ConfigUtils.GetAllCfgObjects(formatType,userid)
  }

    /**
     * Dump the configuration file to the log
     */
  def dumpMetadataAPIConfig {
    ConfigUtils.dumpMetadataAPIConfig
  }

    /**
     * Refresh the ClusterConfiguration for the specified node
     * @param nodeId a cluster node
     * @return
     */
  def RefreshApiConfigForGivenNode(nodeId: String): Boolean = {
    ConfigUtils.RefreshApiConfigForGivenNode(nodeId)
  }

    /**
     * Read metadata api configuration properties
     * @param configFile the MetadataAPI configuration file 
     */
  @throws(classOf[MissingPropertyException])
  @throws(classOf[InvalidPropertyException])
  def readMetadataAPIConfigFromPropertiesFile(configFile: String): Unit = {
    ConfigUtils.readMetadataAPIConfigFromPropertiesFile(configFile)
  }

    /**
     * Read the default configuration property values from json config file.
     * @param cfgFile
     */
  @throws(classOf[MissingPropertyException])
  @throws(classOf[LoadAPIConfigException])
  def readMetadataAPIConfigFromJsonFile(cfgFile: String): Unit = {
    ConfigUtils.readMetadataAPIConfigFromJsonFile(cfgFile)
  }

    /**
     * Initialize the metadata from the bootstrap, establish zookeeper listeners, load the cached information from
     * persistent storage, set up heartbeat and authorization implementations.
     * 
     * @param configFile the MetadataAPI configuration file 
     * @param startHB
     */
  def InitMdMgr(configFile: String, startHB: Boolean) {

    MdMgr.GetMdMgr.truncate
    val mdLoader = new MetadataLoad(MdMgr.mdMgr, "", "", "", "")
    mdLoader.initialize
    if (configFile.endsWith(".json")) {
      MetadataAPIImpl.readMetadataAPIConfigFromJsonFile(configFile)
    } else {
      MetadataAPIImpl.readMetadataAPIConfigFromPropertiesFile(configFile)
    }
    val tmpJarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS")
    val jarPaths = if (tmpJarPaths != null) tmpJarPaths.split(",").toSet else scala.collection.immutable.Set[String]()
    MetadataAPIImpl.OpenDbStore(jarPaths, GetMetadataAPIConfig.getProperty("METADATA_DATASTORE"))
    MetadataAPIImpl.LoadAllObjectsIntoCache
    MetadataAPIImpl.CloseDbStore
    MetadataAPIImpl.InitSecImpl
    if (startHB) InitHearbeat
    initZkListeners(startHB)
  }

    /**
     * Initialize the metadata from the bootstrap, establish zookeeper listeners, load the cached information from
     * persistent storage, set up heartbeat and authorization implementations.
     * FIXME: Is there a difference between this function and InitMdMgr?
     * @see InitMdMgr(String,Boolean)
     * @param configFile the MetadataAPI configuration file 
     * @param startHB
     */
  def InitMdMgrFromBootStrap(configFile: String, startHB: Boolean) {

    MdMgr.GetMdMgr.truncate
    val mdLoader = new MetadataLoad(MdMgr.mdMgr, "", "", "", "")
    mdLoader.initialize
    if (configFile.endsWith(".json")) {
      MetadataAPIImpl.readMetadataAPIConfigFromJsonFile(configFile)
    } else {
      MetadataAPIImpl.readMetadataAPIConfigFromPropertiesFile(configFile)
    }

    initZkListeners(startHB)
    val tmpJarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS")
    val jarPaths = if (tmpJarPaths != null) tmpJarPaths.split(",").toSet else scala.collection.immutable.Set[String]()
    MetadataAPIImpl.OpenDbStore(jarPaths, GetMetadataAPIConfig.getProperty("METADATA_DATASTORE"))
    MetadataAPIImpl.LoadAllObjectsIntoCache
    MetadataAPIImpl.InitSecImpl
    if (startHB) InitHearbeat
    isInitilized = true
    logger.debug("Metadata synching is now available.")

  }

    /**
     * Initialize the heart beat service
     */
  private def InitHearbeat: Unit = {
    InitZooKeeper
    MonitorAPIImpl.initMonitorValues(metadataAPIConfig.getProperty("NODE_ID").toString)
    MonitorAPIImpl.startMetadataHeartbeat
  }

  /**
   * Create a listener to monitor Meatadata Cache
   */
  def initZkListeners(startHB: Boolean): Unit = {
    // Set up a zk listener for metadata invalidation   metadataAPIConfig.getProperty("AUDIT_IMPL_CLASS").trim
    var znodePath = metadataAPIConfig.getProperty("ZNODE_PATH")
    var hbPathEngine = znodePath
    var hbPathMetadata = znodePath
    if (znodePath != null) {
      znodePath = znodePath.trim + "/metadataupdate"
      hbPathEngine = hbPathEngine.trim + "/monitor/engine"
      hbPathMetadata = hbPathMetadata + "/monitor/metadata"
    } else return
    var zkConnectString = metadataAPIConfig.getProperty("ZOOKEEPER_CONNECT_STRING")
    if (zkConnectString != null) zkConnectString = zkConnectString.trim else return

    if (zkConnectString != null && zkConnectString.isEmpty() == false && znodePath != null && znodePath.isEmpty() == false) {
      try {
        CreateClient.CreateNodeIfNotExists(zkConnectString, znodePath)
        zkListener = new ZooKeeperListener
        zkListener.CreateListener(zkConnectString, znodePath, UpdateMetadata, 3000, 3000)
        if (startHB) {
          zkListener.CreatePathChildrenCacheListener(zkConnectString, hbPathEngine, true, MonitorAPIImpl.updateHeartbeatInfo, 3000, 3000)
          zkListener.CreatePathChildrenCacheListener(zkConnectString, hbPathMetadata, true, MonitorAPIImpl.updateHeartbeatInfo, 3000, 3000)
        }
      } catch {
        case e: Exception => {
          logger.error("Failed to initialize ZooKeeper Connection.", e)
          throw e
        }
      }
    }
  }


  /**
   * shutdownZkListener - should be called by application using MetadataAPIImpl directly to disable synching of Metadata cache.
   */
  private def shutdownZkListener: Unit = {
    try {
      CloseZKSession
      if (zkListener != null) {
        zkListener.Shutdown
      }
    } catch {
      case e: Exception => {
        logger.error("Error trying to shutdown zookeeper listener.", e)
        throw e
      }
    }
  }

    /**
     * Shutdown the heart beat monitor
     */
  private def shutdownHeartbeat: Unit = {
    try {
      MonitorAPIImpl.shutdownMonitor
      if (heartBeat != null)
        heartBeat.Shutdown
      heartBeat = null
    } catch {
      case e: Exception => {
        logger.error("Error trying to shutdown Hearbbeat. ", e)
        throw e
      }
    }
  }

  /**
   * Release various resources including heartbeat, dbstore, zk listener, and audit adapter
   * FIXME: What about Security adapter? Should there be a 'release' call on the SecurityAdapter trait?
   */
  def shutdown: Unit = {
    shutdownHeartbeat
    CloseDbStore
    shutdownZkListener
    shutdownAuditAdapter
  }

    /**
     * UpdateMetadata - This is a callback function for the Zookeeper Listener.  It will get called when we detect Metadata being updated from
     *                  a different metadataImpl service.
     * 
     * @param receivedJsonStr message from another cluster node 
     */
  def UpdateMetadata(receivedJsonStr: String): Unit = {
    logger.debug("Process ZooKeeper notification " + receivedJsonStr)
    if (receivedJsonStr == null || receivedJsonStr.size == 0 || !isInitilized) {
      // nothing to do
      logger.debug("Metadata synching is not available.")
      return
    }

    val zkTransaction = JsonSerializer.parseZkTransaction(receivedJsonStr, "JSON")
    MetadataAPIImpl.UpdateMdMgr(zkTransaction)
  }

    /**
     * InitMdMgr
     * @param mgr
     * @param jarPathsInfo
     * @param databaseInfo
     */
  def InitMdMgr(mgr: MdMgr, jarPathsInfo: String, databaseInfo: String) {

    val mdLoader = new MetadataLoad(mgr, "", "", "", "")
    mdLoader.initialize

    metadataAPIConfig.setProperty("JAR_PATHS", jarPathsInfo)
    metadataAPIConfig.setProperty("METADATA_DATASTORE", databaseInfo)

    val tmpJarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS")
    val jarPaths = if (tmpJarPaths != null) tmpJarPaths.split(",").toSet else scala.collection.immutable.Set[String]()
    MetadataAPIImpl.OpenDbStore(jarPaths, GetMetadataAPIConfig.getProperty("METADATA_DATASTORE"))
    MetadataAPIImpl.LoadAllObjectsIntoCache
  }
}
