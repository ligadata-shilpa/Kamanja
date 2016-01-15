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

import org.apache.logging.log4j._
import java.io.File
import com.ligadata.Utils._
import com.ligadata.MigrateBase.{ MigratableFrom, MigratableTo }
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import scala.io.Source

/**
 *
 * {
 * "ClusterConfigFile": "",
 * "ApiConfigFile": "",
 * "MigratingFrom": {
 * "Version": "",
 * "VersionInstallPath": "",
 * "ImplemtedClass": "",
 * "Jars": []
 * },
 * "MigratingTo": {
 * "Version": "",
 * "ImplemtedClass": "",
 * "Jars": []
 * }
 * }
 *
 */

object Migrate {
  case class MigrateFromConfig(version: String, versionInstallPath: String, implemtedClass: String, jars: List[String])
  case class MigrateToConfig(version: String, implemtedClass: String, jars: List[String])
  case class Configuration(clusterConfigFile: String, apiConfigFile: String, migratingFrom: MigrateFromConfig, migratingTo: MigrateToConfig)

  private type OptionMap = Map[Symbol, Any]
  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)

  val allMetadata = ArrayBuffer[(String, String)]()

  private def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    list match {
      case Nil => map
      case "--config" :: value :: tail =>
        nextOption(map ++ Map('config -> value), tail)
      case option :: tail => {
        logger.error("Unknown option " + option)
        sys.exit(1)
      }
    }
  }

  private def usage: Unit = {
    logger.error("Usage: migrate --config <ConfigurationJsonFile>")
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

  private def LoadFqJarsIfNeeded(jars: Array[String], loadedJars: scala.collection.mutable.TreeSet[String], loader: KamanjaClassLoader): Boolean = {
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

  private def isDerivedFrom(clz: Class[_], clsName: String): Boolean = {
    var isIt: Boolean = false

    val interfecs = clz.getInterfaces()
    logger.debug("Interfaces => " + interfecs.length + ",isDerivedFrom: Class=>" + clsName)

    breakable {
      for (intf <- interfecs) {
        val intfName = intf.getName()
        logger.debug("Interface:" + intfName)
        if (intfName.equals(clsName)) {
          isIt = true
          break
        }
      }
    }

    if (isIt == false) {
      val superclass = clz.getSuperclass
      if (superclass != null) {
        val scName = superclass.getName()
        logger.debug("SuperClass => " + scName)
        if (scName.equals(clsName)) {
          isIt = true
        }
      }
    }

    isIt
  }

  private[this] def LoadSourceMigrater(clsName: String, kamanjaLoader: KamanjaLoaderInfo): MigratableFrom = {
    var curClass: Class[_] = null
    var found = false

    try {
      // If required we need to enable this test
      // Convert class name into a class
      var curClz = Class.forName(clsName, true, kamanjaLoader.loader)
      curClass = curClz

      while (curClz != null && found == false) {
        found = isDerivedFrom(curClz, "com.ligadata.MigrateBase.MigratableFrom")
        if (found == false)
          curClz = curClz.getSuperclass()
      }
    } catch {
      case e: Exception => {
        logger.debug("Failed to get classname :" + clsName, e)
        return null
      }
    }

    if (found) {
      try {
        var objinst: Any = null
        try {
          // Trying Singleton Object
          val module = kamanjaLoader.mirror.staticModule(clsName)
          val obj = kamanjaLoader.mirror.reflectModule(module)
          objinst = obj.instance
        } catch {
          case e: Exception => {
            // Trying Regular Object instantiation
            logger.debug("Failed to instatiate object.", e)
            objinst = curClass.newInstance
          }
        }
        if (objinst.isInstanceOf[MigratableFrom]) {
          return objinst.asInstanceOf[MigratableFrom]
        } else {
          logger.error("Failed to instantiate object:" + clsName)
          return null
        }
      } catch {
        case e: Exception => {
          logger.error("Failed to instantiate object:" + clsName, e)
          return null
        }
      }
    }
    return null
  }

  private[this] def LoadDestMigrater(clsName: String, kamanjaLoader: KamanjaLoaderInfo): MigratableTo = {
    var curClass: Class[_] = null
    var found = false

    try {
      // If required we need to enable this test
      // Convert class name into a class
      var curClz = Class.forName(clsName, true, kamanjaLoader.loader)
      curClass = curClz

      while (curClz != null && found == false) {
        found = isDerivedFrom(curClz, "com.ligadata.MigrateBase.MigratableTo")
        if (found == false)
          curClz = curClz.getSuperclass()
      }
    } catch {
      case e: Exception => {
        logger.debug("Failed to get classname :" + clsName, e)
        return null
      }
    }

    if (found) {
      try {
        var objinst: Any = null
        try {
          // Trying Singleton Object
          val module = kamanjaLoader.mirror.staticModule(clsName)
          val obj = kamanjaLoader.mirror.reflectModule(module)
          objinst = obj.instance
        } catch {
          case e: Exception => {
            // Trying Regular Object instantiation
            logger.debug("Failed to instatiate object.", e)
            objinst = curClass.newInstance
          }
        }
        if (objinst.isInstanceOf[MigratableTo]) {
          return objinst.asInstanceOf[MigratableTo]
        } else {
          logger.error("Failed to instantiate object:" + clsName)
          return null
        }
      } catch {
        case e: Exception => {
          logger.error("Failed to instantiate object:" + clsName, e)
          return null
        }
      }
    }
    return null
  }

  private def CollectAllMetadata(typ: String, jsonStr: String): Boolean = {
    logger.info("Got Metadata => Key:%s, JsonString:%s".format(typ, jsonStr))
    allMetadata += ((typ, jsonStr))
    return true
  }

  private def GetConfigurationFromCfgFile(cfgfile: String): Configuration = {
    val cfgStr = Source.fromFile(cfgfile).mkString

    var configMap: Map[String, Any] = null

    try {
      implicit val jsonFormats = DefaultFormats
      val json = parse(cfgStr)
      logger.debug("Valid json: " + cfgStr)

      configMap = json.values.asInstanceOf[Map[String, Any]]
    } catch {
      case e: Exception => {
        logger.error("Failed to parse JSON from input config file:%s.\nInvalid JSON:%s".format(cfgfile, cfgStr), e)
        throw e
      }
    }

    val errSb = new StringBuilder

    val clusterCfgFile = configMap.getOrElse("ClusterConfigFile", "").toString.trim
    if (clusterCfgFile.size == 0) {
      errSb.append("Not found valid ClusterConfigFile key in Configfile:%s\n".format(cfgfile))
    } else {
      val tmpfl = new File(clusterCfgFile)
      if (tmpfl.exists == false || tmpfl.isFile == false) {
        errSb.append("Not found valid ClusterConfigFile key in Configfile:%s\n".format(cfgfile))
      }
    }

    val apiCfgFile = configMap.getOrElse("ApiConfigFile", "").toString.trim
    if (apiCfgFile.size == 0) {
      errSb.append("Not found valid ApiConfigFile key in Configfile:%s\n".format(cfgfile))
    } else {
      val tmpfl = new File(clusterCfgFile)
      if (tmpfl.exists == false || tmpfl.isFile == false) {
        errSb.append("Not found valid ApiConfigFile key in Configfile:%s\n".format(cfgfile))
      }
    }

    var migrateFromConfig: MigrateFromConfig = null
    val migrateFrom = configMap.getOrElse("MigratingFrom", null)
    if (migrateFrom == null) {
      errSb.append("Not found valid MigratingFrom key in Configfile:%s\n".format(cfgfile))
    } else {
      var fromMap: Map[String, Any] = null
      try {
        fromMap = migrateFrom.asInstanceOf[Map[String, Any]]
      } catch {
        case e: Exception => {
          errSb.append("Not found valid MigratingFrom key in Configfile:%s\n".format(cfgfile))
        }
        case e: Throwable => {
          errSb.append("Not found valid MigratingFrom key in Configfile:%s\n".format(cfgfile))
        }
      }

      if (fromMap != null) {
        val version = fromMap.getOrElse("Version", "").toString.trim
        if (version.size == 0) {
          errSb.append("Not found valid Version of MigratingFrom key in Configfile:%s\n".format(cfgfile))
        }

        val versionInstallPath = fromMap.getOrElse("VersionInstallPath", "").toString.trim
        if (versionInstallPath.size == 0) {
          errSb.append("Not found valid VersionInstallPath of MigratingFrom key in Configfile:%s\n".format(cfgfile))
        } else {
          val tmpfl = new File(versionInstallPath)
          if (tmpfl.exists == false || tmpfl.isDirectory == false) {
            errSb.append("Not found valid VersionInstallPath of MigratingFrom key in Configfile:%s\n".format(cfgfile))
          }
        }

        val implemtedClass = fromMap.getOrElse("ImplemtedClass", "").toString.trim
        if (implemtedClass.size == 0) {
          errSb.append("Not found valid ImplemtedClass of MigratingFrom key in Configfile:%s\n".format(cfgfile))
        }

        var jars: List[String] = null
        val tjars = fromMap.getOrElse("Jars", null)
        if (tjars == null) {
          errSb.append("Not found valid Jars of MigratingFrom key in Configfile:%s\n".format(cfgfile))
        } else {
          try {
            jars = tjars.asInstanceOf[List[String]]
          } catch {
            case e: Exception => {
              errSb.append("Not found valid Jars of MigratingFrom key in Configfile:%s\n".format(cfgfile))
            }
            case e: Throwable => {
              errSb.append("Not found valid Jars of MigratingFrom key in Configfile:%s\n".format(cfgfile))
            }
          }
        }
        migrateFromConfig = MigrateFromConfig(version, versionInstallPath, implemtedClass, jars)
      }
    }

    var migrateToConfig: MigrateToConfig = null
    val migrateTo = configMap.getOrElse("MigratingTo", null)
    if (migrateTo == null) {
      errSb.append("Not found valid MigratingTo key in Configfile:%s\n".format(cfgfile))
    } else {
      var toMap: Map[String, Any] = null
      try {
        toMap = migrateTo.asInstanceOf[Map[String, Any]]
      } catch {
        case e: Exception => {
          errSb.append("Not found valid MigratingTo key in Configfile:%s\n".format(cfgfile))
        }
        case e: Throwable => {
          errSb.append("Not found valid MigratingTo key in Configfile:%s\n".format(cfgfile))
        }
      }

      if (toMap != null) {
        val version = toMap.getOrElse("Version", "").toString.trim
        if (version.size == 0) {
          errSb.append("Not found valid Version of MigratingTo key in Configfile:%s\n".format(cfgfile))
        }

        val implemtedClass = toMap.getOrElse("ImplemtedClass", "").toString.trim
        if (implemtedClass.size == 0) {
          errSb.append("Not found valid ImplemtedClass of MigratingTo key in Configfile:%s\n".format(cfgfile))
        }

        var jars: List[String] = null
        val tjars = toMap.getOrElse("Jars", null)
        if (tjars == null) {
          errSb.append("Not found valid Jars of MigratingTo key in Configfile:%s\n".format(cfgfile))
        } else {
          try {
            jars = tjars.asInstanceOf[List[String]]
          } catch {
            case e: Exception => {
              errSb.append("Not found valid Jars of MigratingTo key in Configfile:%s\n".format(cfgfile))
            }
            case e: Throwable => {
              errSb.append("Not found valid Jars of MigratingTo key in Configfile:%s\n".format(cfgfile))
            }
          }
        }
        migrateToConfig = MigrateToConfig(version, implemtedClass, jars)
      }
    }

    if (errSb.size > 0) {
      logger.error(errSb.toString)
      sys.exit(1)
    }

    Configuration(clusterCfgFile, apiCfgFile, migrateFromConfig, migrateToConfig)
  }

  def main(args: Array[String]) {
    var migrateFrom: MigratableFrom = null
    var migrateTo: MigratableTo = null
    try {
      if (args.length == 0) {
        usage
        return
      }

      val backupTblSufix = ".bak"

      val options = nextOption(Map(), args.toList)

      logger.info("keys => " + options.keys)
      logger.info("values => " + options.values)

      val cfgfile = options.getOrElse('config, "").toString.trim
      if (cfgfile.size == 0) {
        logger.error("Input required config file")
        usage
        sys.exit(1)
      }

      if (isValidPath(cfgfile, false, true, "ConfigFile") == false) {
        usage
        sys.exit(1)
      }

      val configuration = GetConfigurationFromCfgFile(cfgfile)

      val srcKamanjaLoader = new KamanjaLoaderInfo

      if (configuration.migratingFrom.jars != null && configuration.migratingFrom.jars.size > 0)
        LoadFqJarsIfNeeded(configuration.migratingFrom.jars.toArray, srcKamanjaLoader.loadedJars, srcKamanjaLoader.loader)

      migrateFrom = LoadSourceMigrater(configuration.migratingFrom.implemtedClass, srcKamanjaLoader)

      if (migrateFrom == null) {
        logger.error("Not able to resolve migrateFromClass:" + configuration.migratingFrom.implemtedClass)
        return
      }

      val dstKamanjaLoader = new KamanjaLoaderInfo

      if (configuration.migratingTo.jars != null && configuration.migratingTo.jars.size > 0)
        LoadFqJarsIfNeeded(configuration.migratingTo.jars.toArray, dstKamanjaLoader.loadedJars, dstKamanjaLoader.loader)

      migrateTo = LoadDestMigrater(configuration.migratingTo.implemtedClass, dstKamanjaLoader)

      if (migrateTo == null) {
        logger.error("Not able to resolve migrateToClass:" + configuration.migratingTo.implemtedClass)
        migrateFrom.shutdown
        return
      }

      logger.debug("apiConfigFile:%s, clusterConfigFile:%s".format(configuration.apiConfigFile, configuration.clusterConfigFile))
      migrateTo.init(configuration.apiConfigFile, configuration.clusterConfigFile)

      val (metadataStoreInfo, dataStoreInfo, statusStoreInfo) = migrateTo.getMetadataStoreDataStoreStatusStoreInfo

      logger.debug("metadataStoreInfo:%s, dataStoreInfo:%s, statusStoreInfo:%s".format(metadataStoreInfo, dataStoreInfo, statusStoreInfo))
      migrateFrom.init(configuration.migratingFrom.versionInstallPath, metadataStoreInfo, dataStoreInfo, statusStoreInfo)

      val allTbls = migrateFrom.getAllMetadataDataStatusTableNames
      // Backup all tables, if they are already not backed up
      var allTblsBackedUp = true
      var tblsToBackUp = ArrayBuffer[(String, String)]()
      var tblsToDrop = ArrayBuffer[String]()

      allTbls.foreach(tbl => {
        val dst = tbl + backupTblSufix
        tblsToBackUp += ((tbl, dst))
        tblsToDrop += tbl
        if (migrateTo.isTableExists(tbl)) {
          if (migrateTo.isTableExists(dst) == false) {
            allTblsBackedUp = false
          }
        }
      })

      // Backup all the tables, if any one of them is missing
      if (allTblsBackedUp == false)
        migrateTo.backupTables(tblsToBackUp.toArray, true)

      // Drop all tables after backup
      migrateTo.dropTables(tblsToDrop.toArray)

      migrateFrom.getAllMetadataObjs(backupTblSufix, CollectAllMetadata)

      val orderMetadata = ArrayBuffer[(String, String)]()
      // Populate orderMetadata from allMetadata in the order we need to import/compile/recompile
      orderMetadata ++= allMetadata

      migrateTo.uploadConfiguration

      migrateTo.addMetadata(orderMetadata.toArray)

      val kSaveThreshold = 10000

      val collectedData = ArrayBuffer[(String, Long, Array[String], Long, Int, String, String)]()
      
      var cntr = 0

      val buildOne = (data: Array[(String, Long, Array[String], Long, Int, String, String)]) => {
        data.foreach(d => {
          println("cntr:%d\n\tContainer:%s\n\tTimePartitionValue:%d\n\tBucketKey:%s\n\tTxnId:%d\n\tRowId:%d\n\tSerializer:%s\n\tJsonData:%s".format(cntr, d._1, d._2, d._3.mkString(","), d._4, d._5, d._6, d._7))
          cntr += 1
        })
        collectedData ++= data
        if (collectedData.size >= kSaveThreshold) {
          migrateTo.populateAndSaveData(collectedData.toArray)
          collectedData.clear()
        }
        true
      }

      migrateFrom.getAllDataObjs(backupTblSufix, orderMetadata.toArray, buildOne)

    } catch {
      case e: Throwable => {
        logger.error("Failed to Migrate", e)
      }
    } finally {
      if (migrateFrom != null)
        migrateFrom.shutdown
      if (migrateTo != null)
        migrateTo.shutdown
    }
  }
}

