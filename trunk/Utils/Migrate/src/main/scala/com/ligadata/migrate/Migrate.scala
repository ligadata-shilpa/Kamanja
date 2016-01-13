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

package com.ligadata.Migrate

import java.util.Properties
import java.io._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.text.ParseException
import scala.Enumeration
import scala.io._
import scala.collection.mutable.ArrayBuffer

import scala.collection.mutable._
import scala.reflect.runtime.{ universe => ru }

import com.ligadata.kamanja.metadata.ObjType._
import com.ligadata.kamanja.metadata._
import com.ligadata.kamanja.metadata.MdMgr._

import com.ligadata.kamanja.metadataload.MetadataLoad

import com.ligadata.keyvaluestore._
import com.ligadata.HeartBeat.HeartBeatUtil
import com.ligadata.StorageBase.{ DataStore, Transaction }
import com.ligadata.KvBase.{ Key, Value, TimeRange }
import com.ligadata.MetadataAPI._

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
import com.ligadata.AuditAdapterInfo._
import com.ligadata.SecurityAdapterInfo.SecurityAdapter
import com.ligadata.keyvaluestore.KeyValueManager
import com.ligadata.Exceptions.StackTrace

import java.util.Date
import org.json4s.jackson.Serialization

import scala.collection.mutable.ArrayBuffer
import scala.io._

import com.ligadata.Migrate.SourceAdapter.V_1_1_X._
import com.ligadata.Utils.{ Utils, KamanjaClassLoader, KamanjaLoaderInfo }
import com.ligadata.tools.SaveContainerDataComponent
import com.ligadata.KamanjaBase.MessageContainerBase

import com.google.gson.Gson
import com.google.gson.GsonBuilder

object Migrate {

  lazy val sysNS = "System"
  // system name space
  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)
  lazy val serializerType = "kryo"
  lazy val serializer = SerializerManager.GetSerializer(serializerType)
  private[this] var _kryoDataSer_V_1_1_X: com.ligadata.Serialize.Serializer = null
  private val kvManagerLoader = new KamanjaLoaderInfo
  private val kamanjaLoader_V_1_1_X = new KamanjaLoaderInfo

  private var jarPaths = collection.immutable.Set[String]()
  private var fromVersionJarPaths = collection.immutable.Set[String]()
  private var fromVersionInstallationPath = ""
  private var baseFileToLoadFromPrevVer = ""

  private type OptionMap = Map[Symbol, Any]

  private var metadataStore: DataStore_V_1_1_X = _
  private var transStore: DataStore_V_1_1_X = _
  private var modelStore: DataStore_V_1_1_X = _
  private var messageStore: DataStore_V_1_1_X = _
  private var containerStore: DataStore_V_1_1_X = _
  private var functionStore: DataStore_V_1_1_X = _
  private var conceptStore: DataStore_V_1_1_X = _
  private var typeStore: DataStore_V_1_1_X = _
  private var otherStore: DataStore_V_1_1_X = _
  private var jarStore: DataStore_V_1_1_X = _
  private var configStore: DataStore_V_1_1_X = _
  private var outputmsgStore: DataStore_V_1_1_X = _
  private var modelConfigStore: DataStore_V_1_1_X = _
  private val V_1_1_X_messagesAndContainers = scala.collection.mutable.Map[String, MessageContainerObjBase_V_1_1_X]()

  private var allDataStore: DataStore_V_1_1_X = _

  def oStore = otherStore

  private var tableStoreMap: Map[String, DataStore_V_1_1_X] = Map()

  private def GetDataStoreHandle_V_1_1_X(jarPaths: collection.immutable.Set[String], dataStoreInfo: String, tableName: String): DataStore_V_1_1_X = {
    try {
      logger.info("Getting DB Connection for dataStoreInfo:%s, tableName:%s".format(dataStoreInfo, tableName))
      return KeyValueManager_V_1_1_X.Get(jarPaths, dataStoreInfo, tableName)
    } catch {
      case e: Exception => {
        throw new CreateStoreFailedException(e.getMessage(), e)
      }
    }
  }

  def OpenDbStore_V_1_1_X(jarPaths: collection.immutable.Set[String], dataStoreInfo: String) {
    try {
      logger.info("Opening datastore")
      metadataStore = GetDataStoreHandle_V_1_1_X(jarPaths, dataStoreInfo, "metadata_objects.bak")
      configStore = GetDataStoreHandle_V_1_1_X(jarPaths, dataStoreInfo, "config_objects.bak")
      jarStore = GetDataStoreHandle_V_1_1_X(jarPaths, dataStoreInfo, "jar_store.bak")
      transStore = GetDataStoreHandle_V_1_1_X(jarPaths, dataStoreInfo, "transaction_id.bak")
      modelConfigStore = GetDataStoreHandle_V_1_1_X(jarPaths, dataStoreInfo, "model_config_objects.bak")

      modelStore = metadataStore
      messageStore = metadataStore
      containerStore = metadataStore
      functionStore = metadataStore
      conceptStore = metadataStore
      typeStore = metadataStore
      otherStore = metadataStore
      outputmsgStore = metadataStore
      tableStoreMap = Map("models" -> modelStore,
        "messages" -> messageStore,
        "containers" -> containerStore,
        "functions" -> functionStore,
        "concepts" -> conceptStore,
        "types" -> typeStore,
        "others" -> otherStore,
        "jar_store" -> jarStore,
        "config_objects" -> configStore,
        "transaction_id" -> transStore,
        "outputmsgs" -> outputmsgStore,
        "model_config_objects" -> modelConfigStore,
        "transaction_id" -> transStore)
    } catch {
      case e: CreateStoreFailedException => {
        throw new CreateStoreFailedException(e.getMessage(), e)
      }
      case e: Exception => {
        throw new CreateStoreFailedException(e.getMessage(), e)
      }
    }
  }

  def KeyAsStr(k: Key_V_1_1_X): String = {
    val k1 = k.toArray[Byte]
    new String(k1)
  }

  def ValueAsStr(v: Value_V_1_1_X): String = {
    val v1 = v.toArray[Byte]
    new String(v1)
  }

  def GetObject(key: Key_V_1_1_X, store: DataStore_V_1_1_X): IStorage = {
    try {
      object o extends IStorage {
        var key = new Key_V_1_1_X;
        var value = new Value_V_1_1_X

        def Key_V_1_1_X = key

        def Value_V_1_1_X = value
        def Construct(k: Key_V_1_1_X, v: Value_V_1_1_X) = {
          key = k;
          value = v;
        }
      }

      var k = key
      logger.info("Get the object from store, key => " + KeyAsStr(k))
      store.get(k, o)
      o
    } catch {
      case e: KeyNotFoundException => {
        throw new ObjectNotFoundException(e.getMessage(), e)
      }
      case e: Exception => {
        throw new ObjectNotFoundException(e.getMessage(), e)
      }
    }
  }

  def GetObject(key: String, store: DataStore_V_1_1_X): IStorage = {
    var k = new Key_V_1_1_X
    for (c <- key) {
      k += c.toByte
    }
    GetObject(k, store)
  }

  def ProcessObject(obj: BaseElemDef, collectOrgMsgAndCntainer: Boolean) {
    try {
      // First collecting 1.1 message/container
      if (collectOrgMsgAndCntainer && obj.IsActive && obj.IsDeleted == false && (obj.isInstanceOf[ContainerDef] || obj.isInstanceOf[MessageDef]))
        Load_V_1_1_X_MessageOrContianer(obj, fromVersionJarPaths, kamanjaLoader_V_1_1_X)

      if ( /* obj.IsActive && */ obj.IsDeleted == false) {
        val key = obj.FullNameWithVer.toLowerCase
        val dispkey = obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version)
        obj match {
          case o: ModelDef => {
            logger.info("The model " + o.FullNameWithVer + " needs to be manually migrated because underlying interfaces have changed")
            logger.debug("Adding the model to the cache: name of the object =>  " + dispkey)
          }
          case o: MessageDef => {
            val msgDefStr = o.objectDefinition
            if (msgDefStr != null) {
              logger.info("Adding the message: name of the object =>  " + dispkey)
              MetadataAPIImpl.AddMessage(msgDefStr, "JSON", None)
            } else {
              logger.debug("Bootstrap object. Ignore it")
            }
          }
          case o: ContainerDef => {
            logger.debug("Adding the container : name of the object =>  " + dispkey)
            val msgDefStr = o.objectDefinition
            if (msgDefStr != null) {
              logger.info("Adding the message: name of the object =>  " + dispkey)
              MetadataAPIImpl.AddContainer(msgDefStr, "JSON", None)
            } else {
              logger.debug("Bootstrap object. Ignore it")
            }
          }
          case o: FunctionDef => {
            val funcKey = o.typeString.toLowerCase
            logger.debug("Adding the function to the cache: name of the object =>  " + funcKey)
          }
          case o: AttributeDef => {
            logger.debug("Adding the attribute to the cache: name of the object =>  " + dispkey)
          }
          case o: ScalarTypeDef => {
            logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          }
          case o: ArrayTypeDef => {
            logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          }
          case o: ArrayBufTypeDef => {
            logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          }
          case o: ListTypeDef => {
            logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          }
          case o: QueueTypeDef => {
            logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          }
          case o: SetTypeDef => {
            logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          }
          case o: TreeSetTypeDef => {
            logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          }
          case o: SortedSetTypeDef => {
            logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          }
          case o: MapTypeDef => {
            logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          }
          case o: ImmutableMapTypeDef => {
            logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          }
          case o: HashMapTypeDef => {
            logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          }
          case o: TupleTypeDef => {
            logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          }
          case o: ContainerTypeDef => {
            logger.debug("Adding the Type to the cache: name of the object =>  " + dispkey)
          }
          case o: OutputMsgDef => {
            logger.trace("Adding the Output Msg to the cache: name of the object =>  " + key)
          }
          case _ => {
            logger.error("ProcessObject is not implemented for objects of type " + obj.getClass.getName)
          }
        }
      }
    } catch {
      case e: AlreadyExistsException => {
        logger.error("Failed to Cache the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "): " + e.getMessage())
      }
      case e: Exception => {
        logger.error("Failed to Cache the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "): " + e.getMessage())
      }
    }
  }

  def MigrateAllMetadata(ds: DataStore_V_1_1_X): Unit = {
    try {
      // Load all metadata objects
      var keys = scala.collection.mutable.Set[Key_V_1_1_X]()
      ds.getAllKeys({ (key: Key_V_1_1_X) => keys.add(key) })
      val keyArray = keys.toArray
      if (keyArray.length == 0) {
        logger.debug("No objects available in the Database")
        return
      }

      val allObjs = ArrayBuffer[Value_V_1_1_X]()
      keyArray.foreach(key => {
        val obj = GetObject(key, ds)
        allObjs += obj.Value_V_1_1_X
      })

      // Loading the base file where we have all the base classes like classes from KamanjaBase, metadata, MetadataAPI, etc
      LoadFqJarsIfNeeded(Array(baseFileToLoadFromPrevVer), kamanjaLoader_V_1_1_X.loadedJars, kamanjaLoader_V_1_1_X.loader)
      if (_kryoDataSer_V_1_1_X == null) {
        _kryoDataSer_V_1_1_X = SerializerManager.GetSerializer("kryo")
        if (_kryoDataSer_V_1_1_X != null && kamanjaLoader_V_1_1_X != null && kamanjaLoader_V_1_1_X.loader != null) {
          _kryoDataSer_V_1_1_X.SetClassLoader(kamanjaLoader_V_1_1_X.loader)
        }
      }

      var baseElemCls = Class.forName("com.ligadata.kamanja.metadata.BaseElemDef", true, kamanjaLoader_V_1_1_X.loader)

      // Types (including Msgs & containers)
      val types = ArrayBuffer[String]()
      // Containers
      // Messages
      // Functions
      // Model Configurations
      // Models

      val gson = new Gson();

      val objsJsons = allObjs.map(o => {
        val mObj = serializer.DeserializeObjectFromByteArray(o.toArray[Byte])
        val gsonBaseStr = gson.toJson(mObj, baseElemCls)
        val gsonStr = gson.toJson(mObj)

        println("BaseString:" + gsonBaseStr)
        println("FullString:" + gsonBaseStr)
      })

      /*
      keyArray.foreach(key => {
        val obj = GetObject(key, ds)
        val mObj = serializer.DeserializeObjectFromByteArray(obj.Value_V_1_1_X.toArray[Byte]).asInstanceOf[BaseElemDef]
        if (mObj != null) {
          ProcessObject(mObj, true)
        }
      })
*/

      // Types (other than Msgs & containers)
      // Containers
      // Messages
      // Functions
      // Model Configurations
      // Models

      // Types (other than Msgs & containers)

    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.info("\nStackTrace:" + stackTrace)
        throw new Exception("Failed to load metadata objects into cache:" + e.getMessage())
      }
    }
  }

  private[this] var _serInfoBufBytes_V_1_1_X = 32

  private def getSerializeInfo_V_1_1_X(tupleBytes: Value_V_1_1_X): String = {
    if (tupleBytes.size < _serInfoBufBytes_V_1_1_X) return ""
    val serInfoBytes = new Array[Byte](_serInfoBufBytes_V_1_1_X)
    tupleBytes.copyToArray(serInfoBytes, 0, _serInfoBufBytes_V_1_1_X)
    return (new String(serInfoBytes)).trim
  }

  private def getValueInfo_V_1_1_X(tupleBytes: Value_V_1_1_X): Array[Byte] = {
    if (tupleBytes.size < _serInfoBufBytes_V_1_1_X) return null
    val valInfoBytes = new Array[Byte](tupleBytes.size - _serInfoBufBytes_V_1_1_X)
    Array.copy(tupleBytes.toArray, _serInfoBufBytes_V_1_1_X, valInfoBytes, 0, tupleBytes.size - _serInfoBufBytes_V_1_1_X)
    valInfoBytes
  }

  private def LoadFqJarsIfNeeded(jars: Array[String], loadedJars: TreeSet[String], loader: KamanjaClassLoader): Boolean = {
    // Loading all jars
    for (j <- jars) {
      logger.debug("Processing Jar " + j.trim)
      val fl = new File(j.trim)
      if (fl.exists) {
        try {
          if (loadedJars(fl.getPath())) {
            logger.debug("Jar " + j.trim + " already loaded to class path.")
          } else {
            loader.addURL(fl.toURI().toURL())
            logger.debug("Jar " + j.trim + " added to class path.")
            loadedJars += fl.getPath()
          }
        } catch {
          case e: Exception => {
            logger.error("Jar " + j.trim + " failed added to class path. Message: " + e.getMessage)
            return false
          }
        }
      } else {
        logger.error("Jar " + j.trim + " not found")
        return false
      }
    }

    true
  }

  private def LoadJarIfNeeded(elem: BaseElem, loadedJars: TreeSet[String], loader: KamanjaClassLoader, jarPaths: collection.immutable.Set[String]): Boolean = {
    if (jarPaths == null) return false

    var retVal: Boolean = true
    var allJars: Array[String] = null

    val jarname = if (elem.JarName == null) "" else elem.JarName.trim

    if (elem.DependencyJarNames != null && elem.DependencyJarNames.size > 0 && jarname.size > 0) {
      allJars = elem.DependencyJarNames :+ jarname
    } else if (elem.DependencyJarNames != null && elem.DependencyJarNames.size > 0) {
      allJars = elem.DependencyJarNames
    } else if (jarname.size > 0) {
      allJars = Array(jarname)
    } else {
      return retVal
    }

    val jars = allJars.map(j => Utils.GetValidJarFile(jarPaths, j))

    return LoadFqJarsIfNeeded(jars, loadedJars, loader)
  }

  private def Load_V_1_1_X_MessageOrContianer(obj: BaseElemDef, jarPaths: collection.immutable.Set[String], kamanjaLoader_V_1_1_X: KamanjaLoaderInfo): Unit = {
    return

    var messageObj: BaseMsgObj_V_1_1_X = null
    var containerObj: BaseContainerObj_V_1_1_X = null

    var isOk = true

    val objFullName = obj.FullName.toLowerCase

    var isMsg = false
    var isContainer = false

    if (isOk) {
      isOk = LoadJarIfNeeded(obj, kamanjaLoader_V_1_1_X.loadedJars, kamanjaLoader_V_1_1_X.loader, jarPaths)
    }

    if (isOk) {
      var clsName = obj.PhysicalName.trim
      if (clsName.size > 0 && clsName.charAt(clsName.size - 1) != '$') // if no $ at the end we are taking $
        clsName = clsName + "$"

      if (isMsg == false) {
        // Checking for Message
        try {
          // Convert class name into a class
          var curClz = Class.forName(clsName, true, kamanjaLoader_V_1_1_X.loader)

          while (curClz != null && isContainer == false) {
            isContainer = Utils.isDerivedFrom(curClz, "com.ligadata.KamanjaBase.BaseContainerObj")
            if (isContainer == false)
              curClz = curClz.getSuperclass()
          }
        } catch {
          case e: Exception => {
            logger.error("Failed to load message class %s with Reason:%s Message:%s".format(clsName, e.getCause, e.getMessage))
          }
        }
      }

      if (isContainer == false) {
        // Checking for container
        try {
          // If required we need to enable this test
          // Convert class name into a class
          var curClz = Class.forName(clsName, true, kamanjaLoader_V_1_1_X.loader)

          while (curClz != null && isMsg == false) {
            isMsg = Utils.isDerivedFrom(curClz, "com.ligadata.KamanjaBase.BaseMsgObj")
            if (isMsg == false)
              curClz = curClz.getSuperclass()
          }
        } catch {
          case e: Exception => {
            logger.error("Failed to load container class %s with Reason:%s Message:%s".format(clsName, e.getCause, e.getMessage))
          }
        }
      }

      if (isMsg || isContainer) {
        try {
          val module = kamanjaLoader_V_1_1_X.mirror.staticModule(clsName)
          val obj = kamanjaLoader_V_1_1_X.mirror.reflectModule(module)
          val objinst = obj.instance
          if (objinst.isInstanceOf[BaseMsgObj_V_1_1_X]) {
            messageObj = objinst.asInstanceOf[BaseMsgObj_V_1_1_X]
            logger.debug("Created Message Object")
            V_1_1_X_messagesAndContainers(objFullName) = messageObj
          } else if (objinst.isInstanceOf[BaseContainerObj_V_1_1_X]) {
            containerObj = objinst.asInstanceOf[BaseContainerObj_V_1_1_X]
            logger.debug("Created Container Object")
            V_1_1_X_messagesAndContainers(objFullName) = containerObj
          } else {
            logger.error("Failed to instantiate message or conatiner object :" + clsName)
            isOk = false
          }
        } catch {
          case e: Exception => {
            logger.error("Failed to instantiate message or conatiner object:" + clsName + ". Reason:" + e.getCause + ". Message:" + e.getMessage())
            isOk = false
          }
        }
      } else {
        logger.error("Failed to instantiate message or conatiner object :" + clsName)
        isOk = false
      }
    }
  }

  object MdResolve_V_1_1_X extends MdBaseResolveInfo_V_1_1_X {
    override def getMessgeOrContainerInstance(MsgContainerType: String): MessageContainerBase_V_1_1_X = {
      val v = V_1_1_X_messagesAndContainers.getOrElse(MsgContainerType.toLowerCase(), null)
      if (v != null && v.isInstanceOf[BaseMsgObj_V_1_1_X]) {
        return v.asInstanceOf[BaseMsgObj_V_1_1_X].CreateNewMessage
      } else if (v != null && v.isInstanceOf[BaseContainerObj_V_1_1_X]) {
        return v.asInstanceOf[BaseContainerObj_V_1_1_X].CreateNewContainer
      }
      return null
    }
  }

  private def migrateFrom_V_1_1_X(tupleBytes: Value_V_1_1_X, storeObjsMap: collection.mutable.Map[String, ArrayBuffer[MessageContainerBase]]): Int = {
    // Get first _serInfoBufBytes bytes
    if (tupleBytes.size < _serInfoBufBytes_V_1_1_X) {
      val errMsg = s"Invalid input. This has only ${tupleBytes.size} bytes data. But we are expecting serializer buffer bytes as of size ${_serInfoBufBytes_V_1_1_X}"
      logger.error(errMsg)
      throw new Exception(errMsg)
    }

    val serInfo = getSerializeInfo_V_1_1_X(tupleBytes)
    var kd: KamanjaData_V_1_1_X = null

    serInfo.toLowerCase match {
      case "kryo" => {
        val valInfo = getValueInfo_V_1_1_X(tupleBytes)
        if (_kryoDataSer_V_1_1_X == null) {
          _kryoDataSer_V_1_1_X = SerializerManager.GetSerializer("kryo")
          if (_kryoDataSer_V_1_1_X != null && kamanjaLoader_V_1_1_X != null && kamanjaLoader_V_1_1_X.loader != null) {
            _kryoDataSer_V_1_1_X.SetClassLoader(kamanjaLoader_V_1_1_X.loader)
          }
        }
        if (_kryoDataSer_V_1_1_X != null) {
          kd = _kryoDataSer_V_1_1_X.DeserializeObjectFromByteArray(valInfo).asInstanceOf[KamanjaData_V_1_1_X]
        }
      }
      case "manual" => {
        val valInfo = getValueInfo_V_1_1_X(tupleBytes)
        val datarec = new KamanjaData_V_1_1_X
        datarec.DeserializeData(valInfo, MdResolve_V_1_1_X, kamanjaLoader_V_1_1_X.loader)
        kd = datarec
      }
      case _ => {
        throw new Exception("Found un-handled Serializer Info: " + serInfo)
      }
    }

    if (kd != null) {
      val typName = kd.GetTypeName
      val bucketKey = kd.GetKey
      val data = kd.GetAllData

      // Copy from Old to New structure.

      return data.size
    }

    return 0
  }

  def Migrate_V_1_1_X_Alldata(ds: DataStore_V_1_1_X, mdCfgFile: String) {
    val kThreasholdToSave = 10000
    var saveComp: SaveContainerDataComponent = null

    try {
      var keys = scala.collection.mutable.Set[Key_V_1_1_X]()
      ds.getAllKeys({ (key: Key_V_1_1_X) => keys.add(key) })
      val keyArray = keys.toArray
      if (keyArray.length == 0) {
        logger.debug("No objects available in the Database")
        return
      }

      val storeObjsMap = collection.mutable.Map[String, ArrayBuffer[MessageContainerBase]]()
      var allDataRecsCnt = 0

      keyArray.foreach(key => {
        val obj = GetObject(key, ds)
        val recsMigrated = migrateFrom_V_1_1_X(obj.Value_V_1_1_X, storeObjsMap)
        allDataRecsCnt += recsMigrated
        if (allDataRecsCnt > kThreasholdToSave) {

          if (saveComp == null) {
            saveComp = new SaveContainerDataComponent
            saveComp.Init(mdCfgFile)
          }
          saveComp.SaveMessageContainerBase(storeObjsMap.map(kv => (kv._1, kv._2.toArray)).toArray, false, true)
          allDataRecsCnt = 0
          storeObjsMap.clear()
        }
      })
    } catch {
      case e: Exception => {
        logger.error("Failed to Migrate Data", e)
        throw e
      }
      case e: Throwable => {
        logger.error("Failed to Migrate Data", e)
        throw e
      }
    } finally {
      if (saveComp != null)
        saveComp.Shutdown
      saveComp = null
    }
  }

  private def BackupMetadata(adapter: DataStore): Unit = {
    try {
      adapter.backupContainer("metadata_objects")
      adapter.backupContainer("jar_store")
      adapter.backupContainer("config_objects")
      adapter.backupContainer("model_config_objects")
      adapter.backupContainer("transaction_id")
      // if we reach this point, we have successfully backed up the container

    } catch {
      case e: Exception => {
        throw new Exception("Failed to backup metadata  " + e.getMessage())
      }
    }
  }

  private def Backupdata(adapter: DataStore): Unit = {
    try {
      adapter.backupContainer("AllData")
      // if we reach this point, we have successfully backed up the data

    } catch {
      case e: Exception => {
        throw new Exception("Failed to backup metadata  " + e.getMessage())
      }
    }
  }

  private def DropMetadata(adapter: DataStore): Unit = {
    try {
      var containers = Array("metadata_objects", "config_objects", "model_config_objects", "jar_store", "transaction_id")
      adapter.DropContainer(containers)
    } catch {
      case e: Exception => {
        throw new Exception("Failed to Drop metadata  " + e.getMessage())
      }
    }
  }

  private def getStringFromJsonNode(v: Any): String = {
    if (v == null) return ""

    if (v.isInstanceOf[String]) return v.asInstanceOf[String]

    implicit val jsonFormats: Formats = DefaultFormats
    val lst = List(v)
    val str = Serialization.write(lst)
    if (str.size > 2) {
      return str.substring(1, str.size - 1)
    }
    return ""
  }

  def GetDataStoreVar(cfgStr: String): String = {
    var dsStr: String = null
    try {
      // extract config objects
      val map = JsonSerializer.parseEngineConfig(cfgStr)
      // process clusterInfo object if it exists
      if (map.contains("Clusters")) {
        val clustersList = map.get("Clusters").get.asInstanceOf[List[_]]
        val clusters = clustersList.length
        logger.debug("Found " + clusters + " cluster objects ")
        clustersList.foreach(clustny => {
          if (dsStr == null) {
            val cluster = clustny.asInstanceOf[Map[String, Any]]
            val ClusterId = cluster.getOrElse("ClusterId", "").toString.trim.toLowerCase
            logger.debug("Processing the cluster => " + ClusterId)
            // gather config name-value pairs
            if (ClusterId.size > 0 && cluster.contains("DataStore"))
              dsStr = getStringFromJsonNode(cluster.getOrElse("DataStore", null))
          }
        })
      }
      dsStr
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        throw new Exception("Failed to parse clusterconfig:\nStackTrace:" + stackTrace)
      }
    }
  }

  private def StartMigrate(clusterCfgFile: String, apiCfgFile: String, fromRelease: String): Unit = {
    try {
      logger.info("Migration started...")
      // read the properties file supplied
      MdMgr.GetMdMgr.truncate
      val mdLoader = new MetadataLoad(MdMgr.mdMgr, "", "", "", "")
      mdLoader.initialize
      MetadataAPIImpl.readMetadataAPIConfigFromPropertiesFile(apiCfgFile)

      val tmpJarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS")
      jarPaths = if (tmpJarPaths != null) tmpJarPaths.split(",").toSet else scala.collection.immutable.Set[String]()
      val metaDataStoreInfo = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("METADATA_DATASTORE");
      val adapter = HBaseAdapter.CreateStorageAdapter(kvManagerLoader, metaDataStoreInfo)
      val cfgStr = Source.fromFile(clusterCfgFile).mkString
      val dataStoreInfo = GetDataStoreVar(cfgStr)
      val dataAdapter = HBaseAdapter.CreateStorageAdapter(kvManagerLoader, dataStoreInfo)
      try {
        BackupMetadata(adapter)
        Backupdata(dataAdapter)
        // if we reach this point, we have successfully backed up the metadata and data

      } catch {
        case e: Exception => {
          throw new Exception("Failed to backup metadata  " + e.getMessage())
        }
      }
      logger.info("Backup finished...")
      logger.info("Update Migration Status...")

      logger.info("Start Reading 1.1.x objects")
      logger.info("Create 1.1.x adapters...")
      OpenDbStore_V_1_1_X(fromVersionJarPaths, metaDataStoreInfo)

      logger.info("Create 1.3 adapters")
      MetadataAPIImpl.OpenDbStore(jarPaths, metaDataStoreInfo)

      // Drop metadata objects in preparation for adding them again
      try {
        DropMetadata(adapter)
      } catch {
        case e: Exception => {
          throw new Exception("Failed to truncate metadata objects " + e.getMessage())
        }
      }
      // Start Migration of objects into new database
      logger.info("Migrate all objects...")
      logger.info("Load Cluster config ..")
      MetadataAPIImpl.UploadConfig(cfgStr, None, "ClusterConfig")
      MigrateAllMetadata(metadataStore)
      allDataStore = GetDataStoreHandle_V_1_1_X(fromVersionJarPaths, dataStoreInfo, "AllData.bak")
      Migrate_V_1_1_X_Alldata(allDataStore, apiCfgFile)
    } catch {
      case e: Exception => {
        throw new Exception("Migration has Failed " + e.getMessage())
      }
    }
  }

  private def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    list match {
      case Nil => map
      case "--clusterconfig" :: value :: tail =>
        nextOption(map ++ Map('clusterconfig -> value), tail)
      case "--apiconfig" :: value :: tail =>
        nextOption(map ++ Map('apiconfig -> value), tail)
      case "--fromversion" :: value :: tail =>
        nextOption(map ++ Map('fromversion -> value), tail)
      case "--fromversioninstallationpath" :: value :: tail =>
        nextOption(map ++ Map('fromversioninstallationpath -> value), tail)
      case option :: tail => {
        logger.error("Unknown option " + option)
        sys.exit(1)
      }
    }
  }

  private def usage: Unit = {
    logger.error("Missing or incorrect arguments Usage: migrate --clusterconfig <your-current-release-config-dir>/clusterconfig.json --apiconfig <your-current-release-config-dir>/MetadataAPIConfig.properties --fromversion 1.x --fromversioninstallationpath <your-previous-release-install-dir> ")
  }

  private def isValidPath(path: String, checkForDir: Boolean = false, checkForFile: Boolean = false, str: String = "path"): Boolean = {
    val fl = new File(path)
    if (fl.exists() == false) {
      logger.error("Given %s:%s does not exists".format(str, path))
      return false
    }

    if (checkForDir && fl.isDirectory() == false) {
      logger.error("Given %s:%s is not directory".format(str, path))
      return false
    }

    if (checkForFile && fl.isFile() == false) {
      logger.error("Given %s:%s is not file".format(str, path))
      return false
    }

    return true
  }

  def main(args: Array[String]) {
    try {
      var clusterCfgFile: String = null
      var apiCfgFile: String = null
      var fromRelease: String = null
      if (args.length == 0) {
        usage
        return
      } else {
        val options = nextOption(Map(), args.toList)

        logger.info("keys => " + options.keys)
        logger.info("values => " + options.values)

        var param = options.getOrElse('clusterconfig, null)
        if (param == null) {
          usage
          return
        }
        clusterCfgFile = param.asInstanceOf[String].trim
        logger.info("clusterCfgFile => " + clusterCfgFile)
        param = options.getOrElse('apiconfig, null)
        if (param == null) {
          usage
          return
        }
        apiCfgFile = param.asInstanceOf[String].trim
        logger.info("apCfgFile => " + apiCfgFile)
        param = options.getOrElse('fromversion, null)
        if (param == null) {
          usage
          return
        }
        fromRelease = param.asInstanceOf[String].trim
        logger.info("fromRelease => " + fromRelease)

        param = options.getOrElse('fromversioninstallationpath, null)
        if (param == null) {
          usage
          return
        }

        val dirPath = param.asInstanceOf[String].trim

        if (isValidPath(dirPath, true, false, "fromversioninstallationpath") == false) {
          usage
          return
        }

        if (isValidPath(dirPath + "/bin", true, false, "bin folder in fromversioninstallationpath") == false) {
          usage
          return
        }

        if (isValidPath(dirPath + "/lib/system", true, false, "/lib/system folder in fromversioninstallationpath") == false) {
          usage
          return
        }

        if (isValidPath(dirPath + "/lib/application", true, false, "/lib/application folder in fromversioninstallationpath") == false) {
          usage
          return
        }

        val installPath = new File(dirPath)

        fromVersionInstallationPath = installPath.getAbsolutePath

        val sysPath = new File(dirPath + "/lib/system")
        val appPath = new File(dirPath + "/lib/application")

        fromVersionJarPaths = collection.immutable.Set[String](sysPath.getAbsolutePath, appPath.getAbsolutePath)

        logger.info("fromVersionInstallationPath:%s, fromVersionJarPaths:%s".format(fromVersionInstallationPath, fromVersionJarPaths.mkString(",")))

        val dir = new File(fromVersionInstallationPath + "/bin");

        val mdapiFls = dir.listFiles.filter(_.isFile).filter(_.getName.startsWith("MetadataAPI-")).toList

        if (mdapiFls.size == 0) {
          val kmFls = dir.listFiles.filter(_.isFile).filter(_.getName.startsWith("KamanjaManager-")).toList
          if (kmFls.size == 0) {
            logger.error("Not found %s/bin/MetadataAPI-* and %s/bin/KamanjaManager-*".format(fromVersionInstallationPath, fromVersionInstallationPath))
            usage
            return
          } else {
            baseFileToLoadFromPrevVer = kmFls(0).getAbsolutePath
          }
        } else {
          baseFileToLoadFromPrevVer = mdapiFls(0).getAbsolutePath
        }
      }
      StartMigrate(clusterCfgFile, apiCfgFile, fromRelease)
    } catch {
      case e: Throwable => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.error("StackTrace:" + stackTrace)
      }
    } finally {
      MetadataAPIImpl.shutdown
    }
  }
}
