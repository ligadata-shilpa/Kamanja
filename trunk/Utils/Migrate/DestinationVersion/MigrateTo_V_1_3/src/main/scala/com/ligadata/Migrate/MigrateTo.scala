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

import com.ligadata.MigrateBase.MigratableTo
import org.apache.logging.log4j._
import java.io.File

import com.ligadata.kamanja.metadata.MdMgr
import com.ligadata.kamanja.metadataload.MetadataLoad
import com.ligadata.MetadataAPI.MetadataAPIImpl
import com.ligadata.Serialize.JsonSerializer
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import scala.io.Source

object MigrateTo_V_1_3 extends MigratableTo {
  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)

  private var _apiConfigFile: String = _
  private var _clusterConfigFile: String = _
  private var _metaDataStoreInfo: String = _
  private var _dataStoreInfo: String = _
  private var _statusStoreInfo: String = _
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
          if (dsStr == null) {
            val cluster = clustny.asInstanceOf[Map[String, Any]]
            val ClusterId = cluster.getOrElse("ClusterId", "").toString.trim.toLowerCase
            logger.debug("Processing the cluster => " + ClusterId)
            if (ClusterId.size > 0 && cluster.contains("DataStore"))
              dsStr = getStringFromJsonNode(cluster.getOrElse("DataStore", null))
          }
          if (ssStr == null) {
            val cluster = clustny.asInstanceOf[Map[String, Any]]
            val ClusterId = cluster.getOrElse("ClusterId", "").toString.trim.toLowerCase
            logger.debug("Processing the cluster => " + ClusterId)
            if (ClusterId.size > 0 && cluster.contains("StatusInfo"))
              ssStr = getStringFromJsonNode(cluster.getOrElse("StatusInfo", null))
          }
        })
      }
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

  override def init(apiConfigFile: String, clusterConfigFile: String): Unit = {
    isValidPath(apiConfigFile, false, true, "apiConfigFile")
    isValidPath(clusterConfigFile, false, true, "clusterConfigFile")

    MdMgr.GetMdMgr.truncate
    val mdLoader = new MetadataLoad(MdMgr.mdMgr, "", "", "", "")
    mdLoader.initialize
    MetadataAPIImpl.readMetadataAPIConfigFromPropertiesFile(apiConfigFile)

    val tmpJarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS")
    val jarPaths = if (tmpJarPaths != null) tmpJarPaths.split(",").toSet else scala.collection.immutable.Set[String]()
    val metaDataStoreInfo = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("METADATA_DATASTORE");
    val cfgStr = Source.fromFile(clusterConfigFile).mkString
    val (dataStoreInfo, statusStoreInfo) = GetDataStoreStatusStoreInfo(cfgStr)

    if (_metaDataStoreInfo == null || _metaDataStoreInfo.size == 0) {
      throw new Exception("Not found valid MetadataStore info in " + apiConfigFile)
    }

    if (dataStoreInfo == null || dataStoreInfo.size == 0) {
      throw new Exception("Not found valid DataStore info in " + clusterConfigFile)
    }

    _apiConfigFile = apiConfigFile
    _clusterConfigFile = clusterConfigFile

    _metaDataStoreInfo = metaDataStoreInfo
    _dataStoreInfo = dataStoreInfo
    _statusStoreInfo = if (statusStoreInfo == null) "" else statusStoreInfo

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

  // the return configuration information is in the order of Metadata Store Info, Data Store Info & Status Store Info
  override def getMetadataStoreDataStoreStatusStoreInfo: (String, String, String) = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
    (_metaDataStoreInfo, _dataStoreInfo, _statusStoreInfo)
  }

  override def isTableExists(tblName: String): Boolean = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
    false
  }

  override def backupTables(tblsToBackedUp: Array[(String, String)], force: Boolean): Unit = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
  }

  override def dropTables(tblsToBackedUp: Array[String]): Unit = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
  }

  // Uploads clusterConfigFile file
  override def uploadConfiguration: Unit = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
    val cfgStr = Source.fromFile(_clusterConfigFile).mkString
    MetadataAPIImpl.UploadConfig(cfgStr, None, "ClusterConfig")
  }

  // Tuple has metadata element type & metadata element data in JSON format.
  override def addMetadata(metadataElemsJson: Array[(String, String)]): Unit = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
  }

  // Array of tuples has container name, timepartition value, bucketkey, transactionid, rowid, serializername & data in Gson (JSON) format
  override def populateAndSaveData(data: Array[(String, Long, Array[String], Long, Int, String, String)]): Unit = {
    if (_bInit == false)
      throw new Exception("Not yet Initialized")
  }

  override def shutdown: Unit = {

  }
}
