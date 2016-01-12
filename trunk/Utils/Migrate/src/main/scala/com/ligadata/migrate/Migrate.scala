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
import scala.util.control.Breaks._
import com.ligadata.AuditAdapterInfo._
import com.ligadata.SecurityAdapterInfo.SecurityAdapter
import com.ligadata.keyvaluestore.KeyValueManager
import com.ligadata.Exceptions.StackTrace

import java.util.Date
import org.json4s.jackson.Serialization

import scala.collection.mutable.ArrayBuffer
import scala.io._

import com.ligadata.Migrate.SourceAdapter.V_1_1_X._

object Migrate {

  lazy val sysNS = "System"
  // system name space
  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)
  lazy val serializerType = "kryo"
  lazy val serializer = SerializerManager.GetSerializer(serializerType)
  private val kvManagerLoader = new KamanjaLoaderInfo

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

  private var allDataStore: DataStore_V_1_1_X = _

  def oStore = otherStore

  private var tableStoreMap: Map[String, DataStore_V_1_1_X] = Map()

  private def GetDataStoreHandle11(jarPaths: collection.immutable.Set[String], dataStoreInfo: String, tableName: String): DataStore_V_1_1_X = {
    try {
      logger.info("Getting DB Connection for dataStoreInfo:%s, tableName:%s".format(dataStoreInfo, tableName))
      return KeyValueManager11.Get(jarPaths, dataStoreInfo, tableName)
    } catch {
      case e: Exception => {
        throw new CreateStoreFailedException(e.getMessage(), e)
      }
    }
  }

  def OpenDbStore11(jarPaths: collection.immutable.Set[String], dataStoreInfo: String) {
    try {
      logger.info("Opening datastore")
      metadataStore = GetDataStoreHandle11(jarPaths, dataStoreInfo, "metadata_objects.bak")
      configStore = GetDataStoreHandle11(jarPaths, dataStoreInfo, "config_objects.bak")
      jarStore = GetDataStoreHandle11(jarPaths, dataStoreInfo, "jar_store.bak")
      transStore = GetDataStoreHandle11(jarPaths, dataStoreInfo, "transaction_id.bak")
      modelConfigStore = GetDataStoreHandle11(jarPaths, dataStoreInfo, "model_config_objects.bak")

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
        def Construct(k: Key_V_1_1_X, v: Value_V_1_1_X) =
          {
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

  def ProcessObject(o: Object) {
    // If the object's Delete flag is set, this is a noop.
    val obj = o.asInstanceOf[BaseElemDef]
    try {
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
    } catch {
      case e: AlreadyExistsException => {
        logger.error("Failed to Cache the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "): " + e.getMessage())
      }
      case e: Exception => {
        logger.error("Failed to Cache the object(" + obj.FullName + "." + MdMgr.Pad0s2Version(obj.Version) + "): " + e.getMessage())
      }
    }
  }

  private def LoadAllModelConfigsIntoChache11(mdMgr: MdMgr): Unit = {
    try {
      var keys = scala.collection.mutable.Set[Key_V_1_1_X]()
      modelConfigStore.getAllKeys({ (key: Key_V_1_1_X) => keys.add(key) })
      val keyArray = keys.toArray
      if (keyArray.length == 0) {
        logger.debug("No model config objects available in the Database")
        return
      }
      keyArray.foreach(key => {
        val obj = GetObject(key, modelConfigStore)
        val conf = serializer.DeserializeObjectFromByteArray(obj.Value_V_1_1_X.toArray[Byte]).asInstanceOf[Map[String, List[String]]]
        mdMgr.AddModelConfig(KeyAsStr(key), conf)
      })
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.info("\nStackTrace:" + stackTrace)
        throw new Exception("Failed to load model configs into cache  " + e.getMessage())
      }
    }
  }

  def LoadAllObjectsIntoCache11(mdMgr: MdMgr) {
    try {
      // Load All the Model Configs here... 
      // LoadAllModelConfigsIntoChache11(mdMgr)

      // Load all metadata objects
      var keys = scala.collection.mutable.Set[Key_V_1_1_X]()
      metadataStore.getAllKeys({ (key: Key_V_1_1_X) => keys.add(key) })
      val keyArray = keys.toArray
      if (keyArray.length == 0) {
        logger.debug("No objects available in the Database")
        return
      }
      keyArray.foreach(key => {
        val obj = GetObject(key, metadataStore)
        val mObj = serializer.DeserializeObjectFromByteArray(obj.Value_V_1_1_X.toArray[Byte]).asInstanceOf[BaseElemDef]
        if (mObj != null) {
          ProcessObject(mObj)
        }
      })
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.info("\nStackTrace:" + stackTrace)
        throw new Exception("Failed to load metadata objects into cache:" + e.getMessage())
      }
    }
  }

  def MigrateAllMetadata(ds: DataStore_V_1_1_X) {
    try {
      // Load all metadata objects
      var keys = scala.collection.mutable.Set[Key_V_1_1_X]()
      ds.getAllKeys({ (key: Key_V_1_1_X) => keys.add(key) })
      val keyArray = keys.toArray
      if (keyArray.length == 0) {
        logger.debug("No objects available in the Database")
        return
      }
      keyArray.foreach(key => {
        val obj = GetObject(key, ds)
        val mObj = serializer.DeserializeObjectFromByteArray(obj.Value_V_1_1_X.toArray[Byte]).asInstanceOf[BaseElemDef]
        if (mObj != null) {
          ProcessObject(mObj)
        }
      })
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.info("\nStackTrace:" + stackTrace)
        throw new Exception("Failed to load metadata objects into cache:" + e.getMessage())
      }
    }
  }

  def MigrateAlldata(ds: DataStore_V_1_1_X) {
    try {
      // Load All the Model Configs here... 
      // LoadAllModelConfigsIntoChache11(mdMgr)

      // Load all metadata objects
      var keys = scala.collection.mutable.Set[Key_V_1_1_X]()
      ds.getAllKeys({ (key: Key_V_1_1_X) => keys.add(key) })
      val keyArray = keys.toArray
      if (keyArray.length == 0) {
        logger.debug("No objects available in the Database")
        return
      }
      keyArray.foreach(key => {
        val obj = GetObject(key, ds)
        val mObj = serializer.DeserializeObjectFromByteArray(obj.Value_V_1_1_X.toArray[Byte]).asInstanceOf[BaseElemDef]
        if (mObj != null) {
          ProcessObject(mObj)
        }
      })
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.info("\nStackTrace:" + stackTrace)
        throw new Exception("Failed to load metadata objects into cache:" + e.getMessage())
      }
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
        logger.debug("Found " + clustersList.length + " cluster objects ")
        clustersList.foreach(clustny => {
          val cluster = clustny.asInstanceOf[Map[String, Any]]
          val ClusterId = cluster.getOrElse("ClusterId", "").toString.trim.toLowerCase
          logger.debug("Processing the cluster => " + ClusterId)
          // gather config name-value pairs
          if (cluster.contains("DataStore")) {
            dsStr = getStringFromJsonNode(cluster.getOrElse("DataStore", null))
            break;
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
      val jarPaths = if (tmpJarPaths != null) tmpJarPaths.split(",").toSet else scala.collection.immutable.Set[String]()
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
      logger.info("Create 1.1.3 adapters...")
      OpenDbStore11(jarPaths, metaDataStoreInfo)

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
      allDataStore = GetDataStoreHandle11(jarPaths, dataStoreInfo, "AllData.bak")
      MigrateAlldata(allDataStore)
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
      case option :: tail => {
        logger.error("Unknown option " + option)
        sys.exit(1)
      }
    }
  }

  private def usage: Unit = {
    logger.error("Missing or incorrect arguments Usage: migrate --clusterconfig <your-current-release-config-dir>/clusterconfig.json --apiconfig <your-current-release-config-dir>/MetadataAPIConfig.properties --fromversion 1.x.x")
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
        clusterCfgFile = param.asInstanceOf[String]
        logger.info("clusterCfgFile => " + clusterCfgFile)
        param = options.getOrElse('apiconfig, null)
        if (param == null) {
          usage
          return
        }
        apiCfgFile = param.asInstanceOf[String]
        logger.info("apCfgFile => " + apiCfgFile)
        param = options.getOrElse('fromversion, null)
        if (param == null) {
          usage
          return
        }
        fromRelease = param.asInstanceOf[String]
        logger.info("fromRelease => " + fromRelease)
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
