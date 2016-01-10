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

object Migrate {

  lazy val sysNS = "System"
  // system name space
  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)
  lazy val serializerType = "kryo"
  lazy val serializer = SerializerManager.GetSerializer(serializerType)
  private val kvManagerLoader = new KamanjaLoaderInfo

  private type OptionMap = Map[Symbol, Any]

  private def StartMigrate(clusterCfgFile:String,apiCfgFile:String,fromRelease:String): Unit = {
    logger.info("Migration started...")
    // read the properties file supplied
    MetadataAPIImpl.readMetadataAPIConfigFromPropertiesFile(apiCfgFile)
    
    val dataStoreInfo = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("METADATA_DATASTORE");
    val adapter = HBaseAdapter.CreateStorageAdapter(kvManagerLoader,dataStoreInfo)
    try{
      adapter.backupContainer("metadata_objects")
      adapter.backupContainer("jar_store")
      adapter.backupContainer("config_objects")
      adapter.backupContainer("model_config_objects")
      adapter.backupContainer("transaction_id")
      // if we reach this point, we have successfully backed up the container
      // do some basic validation
      
    } catch {
      case e: Exception => {
        throw new Exception("Failed to backup metadata  " + e.getMessage())
      }
    }
    logger.info("Migration finished...")
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
      StartMigrate(clusterCfgFile,apiCfgFile,fromRelease)
    } catch {
      case e: Throwable => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        logger.error("StackTrace:"+stackTrace)
      }
    } finally {
      MetadataAPIImpl.shutdown
    }
  }
}
