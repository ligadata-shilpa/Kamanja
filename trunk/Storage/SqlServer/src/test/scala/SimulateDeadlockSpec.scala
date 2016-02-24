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

package com.ligadata.automation.unittests.sqlserveradapter

import org.scalatest._
import Matchers._

import com.ligadata.Utils._
import util.control.Breaks._
import scala.io._
import java.util.{ Date, Calendar, TimeZone }
import java.text.{ SimpleDateFormat }
import java.io._

import sys.process._
import org.apache.log4j._

import com.ligadata.keyvaluestore._
import com.ligadata.KvBase._
import com.ligadata.StorageBase._
import com.ligadata.Serialize._
import com.ligadata.Utils.Utils._
import com.ligadata.Utils.{ KamanjaClassLoader, KamanjaLoaderInfo }
import com.ligadata.StorageBase.StorageAdapterObj
import com.ligadata.keyvaluestore.SqlServerAdapter

import com.ligadata.Exceptions._

case class kafkaoffset(offset: Long)

object SimulateDeadlockSpec {
  private var adapter:DataStore = null
  private val loggerName = this.getClass.getName
  private val logger = LogManager.getLogger(loggerName)
  private[this] val lock = new Object

  val dataStoreInfo = """{"StoreType": "sqlserver","hostname": "192.168.56.1","instancename":"KAMANJA","portnumber":"1433","database": "bofa","user":"bofauser","SchemaName":"bofauser","password":"bofauser","jarpaths":"/media/home2/jdbc","jdbcJar":"sqljdbc4-2.0.jar","clusteredIndex":"YES","autoCreateTables":"YES"}"""

  private val kvManagerLoader = new KamanjaLoaderInfo
  private val maxConnectionAttempts = 10;

  private def CreateAdapter: DataStore = lock.synchronized {
    logger.info("Creating a new adapter")
    var connectionAttempts = 0
    while (connectionAttempts < maxConnectionAttempts) {
      try {
        adapter = SqlServerAdapter.CreateStorageAdapter(kvManagerLoader, dataStoreInfo)
	return adapter
      } catch {
        case e: StorageConnectionException => {
          logger.error("will retry after one minute ...", e)
          connectionAttempts = connectionAttempts + 1
          Thread.sleep(60 * 1000L)
        }
        case e: Exception => {
          logger.error("Failed to connect", e)
          logger.error("retrying ...")
        }
      }
    }
    return null;
  }
  private def getAdapter: DataStore = lock.synchronized {
    if( adapter != null ){
      return adapter
    }
    else{
      return CreateAdapter
    }
  }
}

@Ignore
class SimulateDeadlockSpec extends FunSuite with BeforeAndAfter with BeforeAndAfterAll with ParallelTestExecution {
  var adapter: DataStore = null
  var serializer: Serializer = null
  private[this] val lock = new Object

  private val loggerName = this.getClass.getName
  private val logger = LogManager.getLogger(loggerName)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  
  val containerName = "sys.uniquekvdata"
  var exitImmediately = true

  private def CreateAdapter: DataStore = lock.synchronized {
    return SimulateDeadlockSpec.getAdapter
  }

  before {
    try {
      logger.info("starting...");
      serializer = SerializerManager.GetSerializer("kryo")
      adapter = CreateAdapter
    } catch {
      case e: StorageConnectionException => {
        logger.error("", e)
      }
      case e: Exception => {
        logger.error("Failed to connect", e)
      }
    }
  }

  def deleteFile(path: File): Unit = {
    if (path.exists()) {
      if (path.isDirectory) {
        for (f <- path.listFiles) {
          deleteFile(f)
        }
      }
      path.delete()
    }
  }

  private def GetCurDtTmStr: String = {
    new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date(System.currentTimeMillis))
  }

  private def SimulateDML(keyIndex: Int): Unit = {
    for (batch <- 1 to 1000 ) {
      logger.info("thread: " + keyIndex + ",Batch: " + batch)
      var successful = false
      while ( ! successful ){
	try{
          var keyValueList = new Array[(Key, Value)](0)
	  var dataList = new Array[(String, Array[(Key, Value)])](0)
	  for( p <- 1 to 12 ){
            var keyName = "partition-" + p
            var keyArray = new Array[String](0)
            keyArray = keyArray :+ keyName
            var key = new Key(0, keyArray, 0, 0)
            var offset = 1000000 + batch * p
            var obj = new kafkaoffset(offset)
            var v = serializer.SerializeObjectToByteArray(obj)
            var value = new Value("kryo", v)
            keyValueList = keyValueList :+ (key, value)
	  }
          dataList = dataList :+ (containerName, keyValueList)
	  adapter.put(dataList)
	  successful = true
	}
	catch{
	  case e: Exception => {
	    logger.info("", e)
	    successful = exitImmediately
	  }
	}
      }
    }
  }

  test("thread1") {
    SimulateDML(1)
  }

  test("thread2") {
    SimulateDML(2)
  }

  test("thread3") {
    SimulateDML(3)
  }

  test("thread4") {
    SimulateDML(4)
  }

  test("thread5") {
    SimulateDML(5)
  }

  test("thread6") {
    SimulateDML(6)
  }

  test("thread7") {
    SimulateDML(7)
  }

  test("thread8") {
    SimulateDML(8)
  }

  test("thread9") {
    SimulateDML(9)
  }

  test("thread10") {
    SimulateDML(10)
  }

  test("thread11") {
    SimulateDML(11)
  }

  test("thread12") {
    SimulateDML(12)
  }

  after {
    var logFile = new java.io.File("logs")
    if (logFile != null) {
      deleteFile(logFile)
    }
  }
}
