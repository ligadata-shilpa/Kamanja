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

import com.ligadata.MigrateBase._
import org.apache.logging.log4j._
import java.io.{ File, PrintWriter }

import com.ligadata.kamanja.metadata.MdMgr
import com.ligadata.kamanja.metadataload.MetadataLoad
import com.ligadata.MetadataAPI.MetadataAPIImpl
import com.ligadata.MetadataAPI.MetadataAPI
import com.ligadata.Serialize.JsonSerializer
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import scala.io.Source
// import com.ligadata.tools.SaveContainerDataComponent
import com.ligadata.KvBase.{ Key, Value }
// import com.ligadata.KvBase.{ TimeRange, KvBaseDefalts, KeyWithBucketIdAndPrimaryKey, KeyWithBucketIdAndPrimaryKeyCompHelper, LoadKeyWithBucketId }
import com.ligadata.StorageBase.{ DataStore, Transaction, DataStoreOperations }
import com.ligadata.keyvaluestore.KeyValueManager
import scala.collection.mutable.ArrayBuffer
import com.ligadata.kamanja.metadata.ModelCompilationConstants
import com.ligadata.Exceptions.{ FatalAdapterException, StorageDMLException, StorageDDLException }

class MigrateTo_V_1_3 extends MigratableTo {
  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)

  private var _unhandledMetadataDumpDir: String = _
  private var _sourceVersion: String = _
  private var _destInstallPath: String = _
  private var _apiConfigFile: String = _
  private var _clusterConfigFile: String = _
  private var _metaDataStoreInfo: String = _
  private var _dataStoreInfo: String = _
  private var _statusStoreInfo: String = _
  private var _metaDataStoreDb: DataStore = _
  private var _dataStoreDb: DataStore = _
  private var _statusStoreDb: DataStore = _
  private var _jarPaths: collection.immutable.Set[String] = collection.immutable.Set[String]()
  private var _bInit = false

  private def isValidPath(path: String, checkForDir: Boolean = false, checkForFile: Boolean = false, str: String = "path"): Unit = {
    val fl = new File(path)
    if (fl.exists() == false) {
      val szMsg = "Given %s:%s does not exists".format(str, path)
      logger.error(szMsg)
      throw new Exception(szMsg)
    }

    if (checkForDir && fl.isDirectory() == false) {
      val szMsg = "Given %s:%s is not directory".format(str, path)
      logger.error(szMsg)
      throw new Exception(szMsg)
    }

    if (checkForFile && fl.isFile() == false) {
      val szMsg = "Given %s:%s is not file".format(str, path)
      logger.error(szMsg)
      throw new Exception(szMsg)
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

  private def GetDataStoreStatusStoreInfo(cfgStr: String): (String, String) = {
    var dsStr: String = null
    var ssStr: String = ""
    try {
      // extract config objects
      val map = JsonSerializer.parseEngineConfig(cfgStr)
      // process clusterInfo object if it exists
      if (map.contains("Clusters")) {
        val clustersList = map.get("Clusters").get.asInstanceOf[List[_]]
        val clusters = clustersList.length
        logger.debug("Found " + clusters + " cluster objects ")
        clustersList.foreach(clustny => {
          if (dsStr == null || dsStr.size == 0) {
            val cluster = clustny.asInstanceOf[Map[String, Any]]
            val ClusterId = cluster.getOrElse("ClusterId", "").toString.trim.toLowerCase
            logger.debug("Processing the cluster => " + ClusterId)
            if (ClusterId.size > 0 && cluster.contains("DataStore"))
              dsStr = getStringFromJsonNode(cluster.getOrElse("DataStore", null))
          }
          if (ssStr == null || ssStr.size == 0) {
            val cluster = clustny.asInstanceOf[Map[String, Any]]
            val ClusterId = cluster.getOrElse("ClusterId", "").toString.trim.toLowerCase
            logger.debug("Processing the cluster => " + ClusterId)
            if (ClusterId.size > 0 && cluster.contains("StatusInfo"))
              ssStr = getStringFromJsonNode(cluster.getOrElse("StatusInfo", null))
          }
        })
      }
      logger.debug("Found Datastore String:%s and Statusstore String:%s".format(dsStr, ssStr));
      (dsStr, ssStr)
    } catch {
      case e: Exception => {
        throw new Exception("Failed to parse clusterconfig", e)
      }
      case e: Throwable => {
        throw new Exception("Failed to parse clusterconfig", e)
      }
    }
  }

  private def GetDataStoreHandle(jarPaths: collection.immutable.Set[String], dataStoreInfo: String): DataStore = {
    try {
      logger.debug("Getting DB Connection for dataStoreInfo:%s".format(dataStoreInfo))
      return KeyValueManager.Get(jarPaths, dataStoreInfo)
    } catch {
      case e: Exception => throw e
      case e: Throwable => throw e
    }
  }

  override def init(destInstallPath: String, apiConfigFile: String, clusterConfigFile: String, sourceVersion: String, unhandledMetadataDumpDir: String): Unit = {
    isValidPath(apiConfigFile, false, true, "apiConfigFile")
    isValidPath(clusterConfigFile, false, true, "clusterConfigFile")

    isValidPath(destInstallPath, true, false, "destInstallPath")
    isValidPath(destInstallPath + "/bin", true, false, "bin folder in destInstallPath")
    isValidPath(destInstallPath + "/lib/system", true, false, "/lib/system folder in destInstallPath")
    isValidPath(destInstallPath + "/lib/application", true, false, "/lib/application folder in destInstallPath")

    isValidPath(unhandledMetadataDumpDir, true, false, "unhandledMetadataDumpDir")

    MdMgr.GetMdMgr.truncate
    val mdLoader = new MetadataLoad(MdMgr.mdMgr, "", "", "", "")
    mdLoader.initialize
    MetadataAPIImpl.readMetadataAPIConfigFromPropertiesFile(apiConfigFile)

    val tmpJarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS")
    val jarPaths = if (tmpJarPaths != null) tmpJarPaths.split(",").toSet else scala.collection.immutable.Set[String]()
    val metaDataStoreInfo = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("METADATA_DATASTORE");
    val cfgStr = Source.fromFile(clusterConfigFile).mkString
    val (dataStoreInfo, statusStoreInfo) = GetDataStoreStatusStoreInfo(cfgStr)

    if (metaDataStoreInfo == null || metaDataStoreInfo.size == 0) {
      throw new Exception("Not found valid MetadataStore info in " + apiConfigFile)
    }

    if (dataStoreInfo == null || dataStoreInfo.size == 0) {
      throw new Exception("Not found valid DataStore info in " + clusterConfigFile)
    }

    val sysPath = new File(destInstallPath + "/lib/system")
    val appPath = new File(destInstallPath + "/lib/application")

    val toVersionJarPaths = collection.immutable.Set[String](sysPath.getAbsolutePath, appPath.getAbsolutePath) ++ jarPaths

    _destInstallPath = destInstallPath
    _apiConfigFile = apiConfigFile
    _clusterConfigFile = clusterConfigFile

    _metaDataStoreInfo = metaDataStoreInfo
    _dataStoreInfo = dataStoreInfo
    _statusStoreInfo = if (statusStoreInfo == null) "" else statusStoreInfo
    _jarPaths = toVersionJarPaths
    _sourceVersion = sourceVersion
    _unhandledMetadataDumpDir = unhandledMetadataDumpDir

    // Open the database here
    _metaDataStoreDb = GetDataStoreHandle(toVersionJarPaths, metaDataStoreInfo)
    _dataStoreDb = GetDataStoreHandle(toVersionJarPaths, dataStoreInfo)

    if (_statusStoreInfo.size > 0) {
      _statusStoreDb = GetDataStoreHandle(toVersionJarPaths, _statusStoreInfo)
    }

    _bInit = true
  }

  override def isInitialized: Boolean = _bInit

  override def getMetadataStoreInfo: String = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
    _metaDataStoreInfo
  }

  override def getDataStoreInfo: String = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
    _dataStoreInfo
  }

  override def getStatusStoreInfo: String = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
    _statusStoreInfo
  }

  override def isMetadataTableExists(tblInfo: TableName): Boolean = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
    _metaDataStoreDb.isTableExists(tblInfo.namespace, tblInfo.name)
  }

  override def isDataTableExists(tblInfo: TableName): Boolean = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
    _dataStoreDb.isTableExists(tblInfo.namespace, tblInfo.name)
  }

  override def isStatusTableExists(tblInfo: TableName): Boolean = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
    if (_statusStoreDb != null)
      return _statusStoreDb.isTableExists(tblInfo.namespace, tblInfo.name)
    false
  }

  override def backupMetadataTables(tblsToBackedUp: Array[BackupTableInfo], force: Boolean): Unit = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
    logger.debug("Backup metadata tables:" + tblsToBackedUp.map(t => "(" + t.srcTable + " => " + t.dstTable + ")").mkString(","))
    tblsToBackedUp.foreach(backupTblInfo => {
      _metaDataStoreDb.copyTable(backupTblInfo.namespace, backupTblInfo.srcTable, backupTblInfo.dstTable, force)
    })
  }

  override def backupDataTables(tblsToBackedUp: Array[BackupTableInfo], force: Boolean): Unit = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
    logger.debug("Backup data tables:" + tblsToBackedUp.map(t => "(" + t.srcTable + " => " + t.dstTable + ")").mkString(","))
    tblsToBackedUp.foreach(backupTblInfo => {
      _dataStoreDb.copyTable(backupTblInfo.namespace, backupTblInfo.srcTable, backupTblInfo.dstTable, force)
    })
  }

  override def backupStatusTables(tblsToBackedUp: Array[BackupTableInfo], force: Boolean): Unit = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
    logger.debug("Backup status tables:" + tblsToBackedUp.map(t => "(" + t.srcTable + " => " + t.dstTable + ")").mkString(","))
    if (_statusStoreDb == null && tblsToBackedUp.size > 0)
      throw new Exception("Does not have Status store information")
    tblsToBackedUp.foreach(backupTblInfo => {
      _statusStoreDb.copyTable(backupTblInfo.namespace, backupTblInfo.srcTable, backupTblInfo.dstTable, force)
    })
  }

  override def dropMetadataTables(tblsToDrop: Array[TableName]): Unit = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
    if (tblsToDrop.size > 0) {
      val tblsTuples = tblsToDrop.map(t => (t.namespace, t.name))
      logger.debug("Dropping metadata tables:" + tblsTuples.mkString(","))
      _metaDataStoreDb.dropTables(tblsTuples)
    }
  }

  override def dropDataTables(tblsToDrop: Array[TableName]): Unit = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
    if (tblsToDrop.size > 0) {
      val tblsTuples = tblsToDrop.map(t => (t.namespace, t.name))
      logger.debug("Dropping data tables:" + tblsTuples.mkString(","))
      _dataStoreDb.dropTables(tblsTuples)
    }
  }

  override def dropStatusTables(tblsToDrop: Array[TableName]): Unit = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
    if (tblsToDrop.size > 0) {
      if (_statusStoreDb == null)
        throw new Exception("Does not have Status store information")
      val tblsTuples = tblsToDrop.map(t => (t.namespace, t.name))
      logger.debug("Dropping status tables:" + tblsTuples.mkString(","))
      _statusStoreDb.dropTables(tblsTuples)
    }
  }

  override def dropMessageContainerTablesFromMetadata(allMetadataElemsJson: Array[MetadataFormat]): Unit = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")

    val messagesAndContainers = scala.collection.mutable.Set[String]()

    allMetadataElemsJson.foreach(mdf => {
      val json = parse(mdf.objDataInJson)
      val jsonObjMap = json.values.asInstanceOf[Map[String, Any]]

      val isActiveStr = jsonObjMap.getOrElse("IsActive", "").toString.trim()
      if (isActiveStr.size > 0) {
        val isActive = jsonObjMap.getOrElse("IsActive", "").toString.trim().toBoolean
        if (isActive) {
          if ((mdf.objType == "MessageDef") || (mdf.objType == "ContainerDef")) {
            val namespace = jsonObjMap.getOrElse("NameSpace", "").toString.trim()
            val name = jsonObjMap.getOrElse("Name", "").toString.trim()
            messagesAndContainers += (namespace + "." + name).toLowerCase()
          }
        }
      }
    })

    if (messagesAndContainers.size > 0) {
      logger.debug("Dropping containers:" + messagesAndContainers.mkString(","))
      _dataStoreDb.DropContainer(messagesAndContainers.toArray)
    }
  }

  private def WriteStringToFile(flName: String, str: String): Unit = {
    val out = new PrintWriter(flName, "UTF-8")
    try {
      out.print(str)
    } catch {
      case e: Exception => throw e;
      case e: Throwable => throw e;
    } finally { out.close }
  }

  private def ProcessObject(mdObjs: ArrayBuffer[(String, Map[String, Any])]): Unit = {
    try {
      mdObjs.foreach(mdObj =>
        {
          val objType = mdObj._1

          val namespace = mdObj._2.getOrElse("NameSpace", "").toString.trim()
          val name = mdObj._2.getOrElse("Name", "").toString.trim()
          val dispkey = (namespace + "." + name).toLowerCase

          objType match {
            case "ModelDef" => {
              val mdlType = mdObj._2.getOrElse("ModelType", "").toString
              val objFormat = mdObj._2.getOrElse("ObjectFormat", "").toString
              val mdlDefStr = mdObj._2.getOrElse("ObjectDefinition", "").toString
              val ver = mdObj._2.getOrElse("Version", "0.0.1").toString

              logger.info("Adding model:" + dispkey + ", ModelType:" + mdlType + ", ObjectFormat:" + objFormat)

              if (_sourceVersion.equalsIgnoreCase("1.1")) {
                if ((objFormat.equalsIgnoreCase("JAVA")) || (objFormat.equalsIgnoreCase("scala"))) {
                  val mdlInfo = parse(mdlDefStr).values.asInstanceOf[Map[String, Any]]
                  val defStr = mdlInfo.getOrElse(ModelCompilationConstants.SOURCECODE, "").asInstanceOf[String]
                  // val phyName = mdlInfo.getOrElse(ModelCompilationConstants.PHYSICALNAME, "").asInstanceOf[String]
                  val deps = mdlInfo.getOrElse(ModelCompilationConstants.DEPENDENCIES, List[String]()).asInstanceOf[List[String]]
                  val typs = mdlInfo.getOrElse(ModelCompilationConstants.TYPES_DEPENDENCIES, List[String]()).asInstanceOf[List[String]]

                  var defFl = _unhandledMetadataDumpDir + "/mdldef_" + dispkey + "." + ver + "." + objFormat.toLowerCase()
                  var jsonFl = _unhandledMetadataDumpDir + "/mdlinfo_" + dispkey + "." + ver + "." + objFormat.toLowerCase()

                  val dumpMdlInfoStr = ("ModelInfo" ->
                    ("Dependencies" -> deps) ~
                    ("MessageAndContainers" -> typs) ~
                    ("ModelType" -> mdlType) ~
                    ("ObjectFormat" -> objFormat) ~
                    ("ModelDefinition" -> defStr) ~
                    ("NameSpace" -> namespace) ~
                    ("Name" -> name) ~
                    ("Version" -> ver))

                  WriteStringToFile(defFl, defStr)
                  WriteStringToFile(jsonFl, compact(render(dumpMdlInfoStr)))

                  logger.error("%s type models can not be migrated automatically. Model definition is dumped into %s, more model information dumped to %s.".format(objFormat, defFl, jsonFl))
                } else if (objFormat.equalsIgnoreCase("XML")) {
                  MetadataAPIImpl.AddModel(MetadataAPI.ModelType.fromString("kpmml"), mdlDefStr, None, Some(dispkey), Some(ver))
                }
              } else if (_sourceVersion.equalsIgnoreCase("1.2")) {
                if ((objFormat.equalsIgnoreCase("JAVA")) || (objFormat.equalsIgnoreCase("scala"))) {
                  val mdlInfo = parse(mdlDefStr).values.asInstanceOf[Map[String, Any]]
                  val defStr = mdlInfo.getOrElse(ModelCompilationConstants.SOURCECODE, "").asInstanceOf[String]
                  // val phyName = mdlInfo.getOrElse(ModelCompilationConstants.PHYSICALNAME, "").asInstanceOf[String]
                  val deps = mdlInfo.getOrElse(ModelCompilationConstants.DEPENDENCIES, List[String]()).asInstanceOf[List[String]]
                  val typs = mdlInfo.getOrElse(ModelCompilationConstants.TYPES_DEPENDENCIES, List[String]()).asInstanceOf[List[String]]

                  val mdlConfig = ("MigrationModelConfig_from_1_1_to_1_3" ->
                    ("Dependencies" -> deps) ~
                    ("MessageAndContainers" -> typs))
                  MetadataAPIImpl.UploadModelsConfig(compact(render(mdlConfig)), None, "configuration")
                  MetadataAPIImpl.AddModel(MetadataAPI.ModelType.fromString(objFormat), mdlDefStr, None, Some("MigrationModelConfig_from_1_1_to_1_3"), Some(ver))
                } else if (objFormat.equalsIgnoreCase("XML")) {
                  MetadataAPIImpl.AddModel(MetadataAPI.ModelType.fromString("kpmml"), mdlDefStr, None, Some(dispkey), Some(ver))
                }
              } else {
                logger.error("Not supported any other source migration version other than 1.1 and 1.2")
              }
            }
            case "MessageDef" => {
              val msgDefStr = mdObj._2.getOrElse("ObjectDefinition", "").toString
              if (msgDefStr != null && msgDefStr.size > 0) {
                logger.info("Adding the message:" + dispkey)
                MetadataAPIImpl.AddMessage(msgDefStr, "JSON", None)
              } else {
                logger.debug("Bootstrap object. Ignore it")
              }
            }
            case "ContainerDef" => {
              logger.debug("Adding the container:" + dispkey)
              val msgDefStr = mdObj._2.getOrElse("ObjectDefinition", "").toString
              if (msgDefStr != null && msgDefStr.size > 0) {
                logger.info("Adding the message: name of the object =>  " + dispkey)
                MetadataAPIImpl.AddContainer(msgDefStr, "JSON", None)
              } else {
                logger.debug("Bootstrap object. Ignore it")
              }
            }
            case "FunctionDef" => {
              logger.debug("Adding the function: name of the object =>  " + dispkey)
              //FIXME:: Yet to handle
              logger.error("Not yet handled migrating FunctionDef " + objType)
            }
            /*
            case "AttributeDef" => {
              logger.debug("Adding the attribute: name of the object =>  " + dispkey)
            }
            case "ScalarTypeDef" => {
              logger.debug("Adding the Type: name of the object =>  " + dispkey)
            }
            case "ArrayTypeDef" => {
              logger.debug("Adding the Type: name of the object =>  " + dispkey)
            }
            case "ArrayBufTypeDef" => {
              logger.debug("Adding the Type: name of the object =>  " + dispkey)
            }
            case "ListTypeDef" => {
              logger.debug("Adding the Type: name of the object =>  " + dispkey)
            }
            case "QueueTypeDef" => {
              logger.debug("Adding the Type: name of the object =>  " + dispkey)
            }
            case "SetTypeDef" => {
              logger.debug("Adding the Type: name of the object =>  " + dispkey)
            }
            case "TreeSetTypeDef" => {
              logger.debug("Adding the Type: name of the object =>  " + dispkey)
            }
            case "SortedSetTypeDef" => {
              logger.debug("Adding the Type: name of the object =>  " + dispkey)
            }
            case "MapTypeDef" => {
              logger.debug("Adding the Type: name of the object =>  " + dispkey)
            }
            case "ImmutableMapTypeDef" => {
              logger.debug("Adding the Type: name of the object =>  " + dispkey)
            }
            case "HashMapTypeDef" => {
              logger.debug("Adding the Type: name of the object =>  " + dispkey)
            }
            case "TupleTypeDef" => {
              logger.debug("Adding the Type: name of the object =>  " + dispkey)
            }
            case "ContainerTypeDef" => {
              logger.debug("Adding the Type: name of the object =>  " + dispkey)
            }
*/
            case "OutputMsgDef" => {
              logger.debug("Adding the Output Msg: name of the object =>  " + dispkey)
              //FIXME:: Yet to handle
              logger.error("Not yet handled migrating OutputMsgDef " + objType)
            }
            case _ => {
              logger.error("ProcessObject is not implemented for objects of type " + objType)
            }
          }
        })
    } catch {
      case e: Exception => throw e
      case e: Throwable => throw e
    }
  }
  
  override def addMetadata(allMetadataElemsJson: Array[MetadataFormat], uploadClusterConfig: Boolean): Unit = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")

    // Order metadata to add in the given order.
    // First get all the message & containers And also the excluded types we automatically add when we add messages & containers
    val allTemp = ArrayBuffer[(String, Map[String, Any])]()
    val types = ArrayBuffer[(String, Map[String, Any])]()
    val messages = ArrayBuffer[(String, Map[String, Any])]()
    val containers = ArrayBuffer[(String, Map[String, Any])]()
    val functions = ArrayBuffer[(String, Map[String, Any])]()
    val mdlConfig = ArrayBuffer[(String, Map[String, Any])]()
    val models = ArrayBuffer[(String, Map[String, Any])]()
    val jarDef = ArrayBuffer[(String, Map[String, Any])]()
    val outputMsgDef = ArrayBuffer[(String, Map[String, Any])]()
    val configDef = ArrayBuffer[(String, Map[String, Any])]()
    val typesToIgnore = scala.collection.mutable.Set[String]()

    allMetadataElemsJson.foreach(mdf => {
      val json = parse(mdf.objDataInJson)
      val jsonObjMap = json.values.asInstanceOf[Map[String, Any]]

      val isActiveStr = jsonObjMap.getOrElse("IsActive", "").toString.trim()
      if (isActiveStr.size > 0) {
        val isActive = jsonObjMap.getOrElse("IsActive", "").toString.trim().toBoolean
        if (isActive) {
          if (mdf.objType == "MessageDef") {

            val namespace = jsonObjMap.getOrElse("NameSpace", "").toString.trim()
            val name = jsonObjMap.getOrElse("Name", "").toString.trim()

            typesToIgnore += (namespace + ".arrayof" + name).toLowerCase
            typesToIgnore += (namespace + ".arraybufferof" + name).toLowerCase
            typesToIgnore += (namespace + ".sortedsetof" + name).toLowerCase
            typesToIgnore += (namespace + ".immutablemapofintarrayof" + name).toLowerCase
            typesToIgnore += (namespace + ".immutablemapofstringarrayof" + name).toLowerCase
            typesToIgnore += (namespace + ".arrayofarrayof" + name).toLowerCase
            typesToIgnore += (namespace + ".mapofstringarrayof" + name).toLowerCase
            typesToIgnore += (namespace + ".mapofintarrayof" + name).toLowerCase
            typesToIgnore += (namespace + ".setof" + name).toLowerCase
            typesToIgnore += (namespace + ".treesetof" + name).toLowerCase

            messages += ((mdf.objType, jsonObjMap))
          } else if (mdf.objType == "ContainerDef") {

            val namespace = jsonObjMap.getOrElse("NameSpace", "").toString.trim()
            val name = jsonObjMap.getOrElse("Name", "").toString.trim()

            typesToIgnore += (namespace + ".arrayof" + name).toLowerCase
            typesToIgnore += (namespace + ".arraybufferof" + name).toLowerCase
            typesToIgnore += (namespace + ".sortedsetof" + name).toLowerCase
            typesToIgnore += (namespace + ".immutablemapofintarrayof" + name).toLowerCase
            typesToIgnore += (namespace + ".immutablemapofstringarrayof" + name).toLowerCase
            typesToIgnore += (namespace + ".arrayofarrayof" + name).toLowerCase
            typesToIgnore += (namespace + ".mapofstringarrayof" + name).toLowerCase
            typesToIgnore += (namespace + ".mapofintarrayof" + name).toLowerCase
            typesToIgnore += (namespace + ".setof" + name).toLowerCase
            typesToIgnore += (namespace + ".treesetof" + name).toLowerCase

            containers += ((mdf.objType, jsonObjMap))
          } else {
            allTemp += ((mdf.objType, jsonObjMap))
          }
        }
      }
    })

    allTemp.foreach(jsonObjMap => {
      val objType = jsonObjMap._1
      if (objType == "ModelDef") {
        models += jsonObjMap
      } else if (objType == "ArrayTypeDef" ||
        objType == "ArrayBufTypeDef" ||
        objType == "SortedSetTypeDef" ||
        objType == "ImmutableMapTypeDef" ||
        objType == "MapTypeDef" ||
        objType == "HashMapTypeDef" ||
        objType == "SetTypeDef" ||
        objType == "ImmutableSetTypeDef" ||
        objType == "TreeSetTypeDef" ||
        objType == "JarDef" ||
        objType == "OutputMsgDef" ||
        objType == "ConfigDef") {
        val namespace = jsonObjMap._2.getOrElse("NameSpace", "").toString.trim()
        val name = jsonObjMap._2.getOrElse("Name", "").toString.trim()

        val typ = (namespace + "." + name).toLowerCase
        if (typesToIgnore.contains(typ) == false)
          types += jsonObjMap
      } else if (objType == "FunctionDef") {
        functions += jsonObjMap
      } else if (objType == "JarDef") {
        jarDef += jsonObjMap
      } else if (objType == "OutputMsgDef") {
        outputMsgDef += jsonObjMap
      } else if (objType == "ConfigDef") {
        configDef += jsonObjMap
      } else {
        logger.error("ObjectType:%s is not handled".format(objType))
      }
    })

    // Open OpenDbStore
    MetadataAPIImpl.OpenDbStore(_jarPaths, _metaDataStoreInfo)

    val cfgStr = Source.fromFile(_clusterConfigFile).mkString
    logger.debug("Uploading configuration")
    MetadataAPIImpl.UploadConfig(cfgStr, None, "ClusterConfig")

    // We need to add the metadata in the following order
    // Jars
    // Types
    // Containers
    // Messages
    // Functions
    // Model configuration
    // Models
    // OutputMessageDef

    ProcessObject(jarDef)
    ProcessObject(types)
    ProcessObject(containers)
    ProcessObject(messages)
    ProcessObject(functions)
    ProcessObject(configDef)
    ProcessObject(models)
    ProcessObject(outputMsgDef)
  }

  private def callSaveData(dataStore: DataStoreOperations, data_list: Array[(String, Array[(Key, Value)])]): Unit = {
    var failedWaitTime = 15000 // Wait time starts at 15 secs
    val maxFailedWaitTime = 60000 // Max Wait time 60 secs
    var doneSave = false

    while (!doneSave) {
      try {
        dataStore.put(data_list)
        doneSave = true
      } catch {
        case e: FatalAdapterException => {
          logger.error("Failed to save data into datastore", e)
        }
        case e: StorageDMLException => {
          logger.error("Failed to save data into datastore", e)
        }
        case e: StorageDDLException => {
          logger.error("Failed to save data into datastore", e)
        }
        case e: Exception => {
          logger.error("Failed to save data into datastore", e)
        }
        case e: Throwable => {
          logger.error("Failed to save data into datastore", e)
        }
      }

      if (!doneSave) {
        try {
          logger.error("Failed to save data into datastore. Waiting for another %d milli seconds and going to start them again.".format(failedWaitTime))
          Thread.sleep(failedWaitTime)
        } catch {
          case e: Exception => {

          }
        }
        // Adjust time for next time
        if (failedWaitTime < maxFailedWaitTime) {
          failedWaitTime = failedWaitTime * 2
          if (failedWaitTime > maxFailedWaitTime)
            failedWaitTime = maxFailedWaitTime
        }
      }
    }
  }

  // Array of tuples has container name, timepartition value, bucketkey, transactionid, rowid, serializername & data in Gson (JSON) format
  override def populateAndSaveData(data: Array[DataFormat]): Unit = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
    val containersData = data.groupBy(_.containerName.toLowerCase)
    val data_list = containersData.map(kv => (kv._1, kv._2.map(d => (Key(d.timePartition, d.bucketKey, d.transactionid, d.rowid), Value(d.serializername, d.data))).toArray)).toArray

    callSaveData(_dataStoreDb, data_list);
  }

  override def shutdown: Unit = {
    if (_metaDataStoreDb != null)
      _metaDataStoreDb.Shutdown()
    if (_dataStoreDb != null)
      _dataStoreDb.Shutdown()
    if (_statusStoreDb != null)
      _statusStoreDb.Shutdown()
    _metaDataStoreDb = null
    _dataStoreDb = null
    _statusStoreDb = null
    MetadataAPIImpl.shutdown
  }
}
