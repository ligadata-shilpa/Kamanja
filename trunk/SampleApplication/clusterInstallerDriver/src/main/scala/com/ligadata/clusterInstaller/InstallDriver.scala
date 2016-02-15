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

package com.ligadata.clusterInstaller

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.io.Source
import scala.sys.process._

import java.io._
import java.util.regex.{Matcher, Pattern}

import org.apache.logging.log4j.LogManager
import org.json4s._

import com.ligadata.Serialize.JsonSerializer

/**
  * This application installs and upgrades Kamanaja.  It does these essential things:

  * 1)  It verifies that a valid environment is available on the cluster nodes specified in the configuration.  The validation includes determination
  * if the nodes mentioned in the node configuration exist, whether they are configured with the apporopriate scala and java, can access a valid
  * zookeeper cluster, can access a valid kafka cluster and can use the hBase kv store configured for this cluster.
  * 2)  The cluster installation script is used to install a new version of the Kamanja release binariesa in the new installation directory on each node.
  * 3)  If an existing cluster installation is being upgraded, the appropriate migration script is invoked.  A migration json file is prepared with the essential
  * details required for this migration.

  * Notes:
  * 1) The prior installation directory, if upgrading, and the new installation directory are determined from the apiConfig property, ROOT_DIR.  To
  * make sure we no where the new and old installations are located (they are sibling directories in the same parent folder), the parent folder is
  * supplied to the cluster install script.
  * 2) The migration script requires a JSON file that contains information about the upgrade.  It looks like this:

  * {
  * "clusterConfigFile": "{NewPackageInstallPath}/config/ClusterConfig.json",
  * "apiConfigFile": "{NewPackageInstallPath}/config/MetadataAPIConfig.properties",
  * "unhandledMetadataDumpDir": "{UnhandledMetadataDumpDir}",
  * "dataSaveThreshold": 1000,
  * "migratingFrom": {
  * "version": "1.1",
  * "versionInstallPath": "{OldPackageInstallPath}",
  * "implemtedClass": "com.ligadata.Migrate.MigrateFrom_V_1_1",
  * "jars": [
  * "{NewPackageInstallPath}/lib/system/migratebase-1.0.jar",
  * "{NewPackageInstallPath}/lib/system/migratefrom_v_1_1_2.10-1.0.jar",
  * "{OldPackageInstallPath}/bin/KamanjaManager-1.0"
  * ]
  * },
  * "migratingTo": {
  * "version": "1.3",
  * "versionInstallPath": "{NewPackageInstallPath}",
  * "implemtedClass": "com.ligadata.Migrate.MigrateTo_V_1_3",
  * "jars": [
  * "{NewPackageInstallPath}/lib/system/migratebase-1.0.jar",
  * "{NewPackageInstallPath}/lib/system/migrateto_v_1_3_2.11-1.0.jar",
  * "{NewPackageInstallPath}/bin/KamanjaManager-1.0"
  * ]
  * },
  * "excludeMetadata": [
  * "ModelDef",
  * "MessageDef",
  * "ContainerDef",
  * "ConfigDef",
  * "FunctionDef",
  * "JarDef",
  * "OutputMsgDef"
  * ],
  * "excludeData": false
  * }

  * where {NewPackageInstallPath} will be substituted with the new installation directory found in the parent folder.
  * {OldPackageInstallPath} will be substituted with the existing installation directory in the parent folder.
  * 3) This installation process will be discretely logged in a file whose file path is displayed to the console.  All critical details about
  * the steps taken in the installation/upgrade will be noted.  Should failures occur, the log should be consulted for what went wrong and instruction
  * on how to roll back the an upgrade to the prior state if appropriate.  For new installations, simply rerunning the script after correcting the
  * issues in the environment that possibly created the failure should be sufficient.
 */

class InstallDriverLog(logPath : String) {

  val bufferedWriter = new BufferedWriter(new FileWriter(new File(logPath)))

  /** open the discrete log file if possible.  If open fails (file must not previously exist) false is returned */
  def initialize : Boolean = {
      (bufferedWriter != null)
  }

  def emit(msg : String) : Unit = {
      /** write the string as a line to the file */
      bufferedWriter.write(msg)

  } 

  def close : Unit = {
     bufferedWriter.close
  }

}

object InstallDriver extends App {
    lazy val loggerName = this.getClass.getName
    lazy val logger = LogManager.getLogger(loggerName)

    def usage: String = {
    """
    com.ligadata.clusterInstaller.Driver_1_3 --{upgrade|install} --apiConfig <MetadataAPIConfig.properties file> --clusterConfig <ClusterConig.json file> --fromKamanja "1.1" [--fromScala "2.10"] [--toScala "2.11"] --workingDir <workingdirectory> --clusterId <id> --tarballPath <tarball path>

    where
        --upgrade explicitly specifies that the intent to upgrade an existing cluster installation with the latest release.
        --install explicitly specifies that the intent that this is to be a new installation.  If there is an existing implementation and
            the --install option is given, the installation will fail.  Similarly, if there is NO existing implementation
            and the --upgrade option has been given, the upgrade will fail. If both --upgrade and --install are given, the install fails.
        --apiConfig <MetadataAPIConfig.properties file> specifies the path of the properties file that (among others) specifies the location
            of the installation on each cluster node given in the cluster configuration.  Note that all installations exist in the same
            location on all nodes.
        --clusterConfig <ClusterConig.json file> gives the file that describes the node information for the cluster, including the IP address of each node, et al.
        --fromKamanja "N.N" where "N.N" can be either "1.1" or "1.2"
        --workingDir <workingdirectory> a CRUD directory path that should be addressable on every node specified in the cluster configuration
            file.  It is used by the script to create intermediate files used during the installation process.
        --clusterId <id> describes the key that should be used to extract the cluster metadata from the node configuration.  Note is possible to
            have multiple clusters in the metadata.
        --tarballPath <tarball path> this is the location of the Kamanja 1.3 tarball of the installation directory to be installed
            on the cluster.  The tarball is copied to each of the cluster nodes, extracted and installed there.
        [--fromScala "2.10"] an optional parameter that, for the 1.3 InstallDriver, simply documents the version of Scala that the current 1.1. or 1.2 is using.  The value
            "2.10" is the only possible value for this release.
        [--toScala "2.11"] an optional parameter that when given declares the intent to upgrade the Scala compiler used by the Kamanja engine to 2.11.x
            instead of using the default 2.10.x compiler.  Note that this controls the compiler version that the Kamanja message compiler, Kamanja pmml
            compiler, and other future Kamanja components will use when building their respective objects. If the requested version has not been installed
            on the cluster nodes in question, the installation will fail.

    The ClusterInstaller Driver_1_3 is the cluster installer driver for Kamanja 1.3.  It is capable of installing a new version of 1.3 or given the appropriate arguments,
    installing a new version of Kamanja 1.3 *and* upgrading a 1.1 or 1.2 installation to the 1.3 version.

    A log of the installation and optional upgrade is collected in a log file.  This log file is automatically generated and will be found in the
    workingDir you supply to the installer driver program.  The name will be of the form: InstallDriver.yyyyMMMdd_HHmmss.log (e.g.,
    InstallDriver.2016Jul01_231101.log)  Should issues be encountered (missing components, connectivity issues, etc.) this log should be
    consulted as to how to proceed.  Using the content of the log as a guide, the administrator will see what fixes must be made to their
    environment to push on and complete the install.  It will also be the guide for backing off/abandoning an upgrade so a prior Kamanja cluster
    can be restored.

    """
    }

    override def main(args: Array[String]): Unit = {

    if (args.length == 0) { println("Usage:\n"); println(usage); sys.exit(1) }
    val arglist = args.toList
    type OptionMap = Map[Symbol, String]

    //println(s"arguments supplied are:\n $arglist")
    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--clusterId" :: value :: tail =>
          nextOption(map ++ Map('clusterId -> value), tail)
        case "--apiConfig" :: value :: tail =>
          nextOption(map ++ Map('apiConfig -> value), tail)
        case "--clusterConfig" :: value :: tail =>
          nextOption(map ++ Map('clusterConfig -> value), tail)
        case "--tarballPath" :: value :: tail =>
          nextOption(map ++ Map('tarballPath -> value), tail)
        case "--fromKamanja" :: value :: tail =>
          nextOption(map ++ Map('fromKamanja -> value), tail)
        case "--fromScala" :: value :: tail =>
          nextOption(map ++ Map('fromScala -> value), tail)
        case "--toScala" :: value :: tail =>
          nextOption(map ++ Map('toScala -> value), tail)
        case "--workingDir" :: value :: tail =>
          nextOption(map ++ Map('workingDir -> value), tail)
        case "--upgrade" :: tail =>
                               nextOption(map ++ Map('upgrade -> "true"), tail)
        case "--install" :: tail =>
                               nextOption(map ++ Map('install -> "true"), tail)
        case option :: tail =>
          println("Unknown option " + option)
          println(usage)
          sys.exit(1)
      }
    }

    val options = nextOption(Map(), arglist)

    val clusterId : String = if (options.contains('clusterId)) options.apply('clusterId) else null
    val apiConfigPath : String = if (options.contains('apiConfig)) options.apply('apiConfig) else null
    val nodeConfigPath : String = if (options.contains('clusterConfig)) options.apply('clusterConfig) else null
    val tarballPath : String = if (options.contains('tarballPath)) options.apply('tarballPath) else null
    val fromKamanja : String = if (options.contains('fromKamanja)) options.apply('fromKamanja) else null
    val fromScala : String = if (options.contains('fromScala)) options.apply('fromScala) else "2.10"
    val toScala : String = if (options.contains('toScala)) options.apply('toScala) else "2.11"
    val workingDir : String = if (options.contains('workingDir)) options.apply('workingDir) else null
    val upgrade : Boolean = if (options.contains('upgrade)) options.apply('upgrade) == "true" else false
    val install : Boolean = if (options.contains('install)) options.apply('install) == "true" else false

    /** FIXME: we might want to create more meaningful failure messages here that pinpoint the complaint instead
      * of the "your arguments are not satisfactory...Usage:"   */
    val confusedIntention : Boolean = (upgrade && install)
    val reasonableArguments: Boolean = (
        clusterId != null && clusterId.nonEmpty
        && apiConfigPath != null && apiConfigPath.nonEmpty
        && nodeConfigPath != null && nodeConfigPath.nonEmpty
        && tarballPath != null && tarballPath.nonEmpty
        && fromKamanja != null && fromKamanja.nonEmpty && (fromKamanja == "1.1" || fromKamanja == "1.2")
        && fromScala != null && fromScala.nonEmpty && fromScala == "2.10"
        && toScala != null && toScala.nonEmpty && (toScala == "2.10" || toScala == "2.11")
        && workingDir != null && workingDir.nonEmpty
        && ! confusedIntention)

    if (! reasonableArguments) {
        println("Your arguments are not satisfactory...Usage:")
        println(usage)
        sys.exit(1)
    }

    /** make a log ... FIXME: generate a timestamp for the "SomeDate" in the file path below... maybe make better configurable path */
    val dateTime : DateTime = new DateTime
    val fmt : DateTimeFormatter  = DateTimeFormat.forPattern("yyyyMMMdd_HHmmss")
    val datestr : String = fmt.print(dateTime);

    val log : InstallDriverLog = new InstallDriverLog(s"$workingDir/InstallDriver.$datestr.log")

    /** Convert the content of the property file into a map.  If the path is bad, an empty map is returned and processing stops */
    val apiConfigMap : Map[String,String] = mapProperties(log, apiConfigPath)
    if (apiConfigMap.isEmpty) {
        println("The configuration file is messed up... it needs to be lines of key=value pairs")
        log.emit("The configuration file is messed up... it needs to be lines of key=value pairs")
        println(usage)
        log.emit(usage)
        log.close
        sys.exit(1)
    }

    /** Ascertain what the name of the new installation directory will be, what the name of the prior installation would be (post install), and the parent
      * directory in which both of them will live on each cluster node. Should there be no existing installation, the prior installation value will be null. */
    val (parentPath, priorInstallDirName, newInstallDirName) : (String, String, String) = CreateInstallationNames(log, apiConfigMap)

    /** Run the node info extract on the supplied file and garner all of the information needed to conduct the cluster environment validatation */
    val installDir : String = s"$parentPath/$newInstallDirName"

    val clusterConfig : String = Source.fromFile(nodeConfigPath).mkString
    val clusterConfigMap : ClusterConfigMap = new ClusterConfigMap(clusterConfig, clusterId)


    val clusterMap : Map[String,Any] = clusterConfigMap.ClusterMap
    if (clusterMap.isEmpty) {
        log.emit(s"There is no cluster info for the supplied clusterId, $clusterId")
        sys.exit(1)
    }
    val clusterIdFromConfig : String = clusterConfigMap.ClusterId
    val dataStore : Map[String,Any] = clusterConfigMap.DataStore
    val zooKeeperInfo : Map[String,Any] = clusterConfigMap.ZooKeeperInfo
    val environmentContext : Map[String,Any] = clusterConfigMap.EnvironmentContext
    val clusterNodes : List[Map[String,Any]] = clusterConfigMap.ClusterNodes
    val adapters : List[Map[String,Any]] = clusterConfigMap.Adapters

    /* Collect the node information needed to valididate the implied cluster environment */
    val (ips, ipIdTargPaths, ipPathPairs) : (Array[String], Array[(String,String,String,String)], Array[(String,String)]) =
            collectNodeInfo(log, clusterId, clusterNodes, installDir)

    /** Validate that the proposed installation has the requisite characteristics (scala, java, zookeeper, kafka, and hbase) */
    val proposedClusterEnvironmentIsSuitable  : Boolean = validateClusterEnvironment(log
                                                                                  , apiConfigMap
                                                                                  , nodeConfigPath
                                                                                  , fromKamanja
                                                                                  , fromScala
                                                                                  , toScala
                                                                                  , upgrade
                                                                                  , install
                                                                                  , parentPath
                                                                                  , priorInstallDirName
                                                                                  , newInstallDirName
                                                                                  , ips, ipIdTargPaths, ipPathPairs)
    if (proposedClusterEnvironmentIsSuitable) {

        /** Install the new installation */
        val nodes : String = " some nodes " //clusterNodes(ips)
        log.emit(s"Begin cluster installation... installation found on each cluster node(any {$nodes}) at $installDir")
        val installOk : Boolean = installCluster(log, apiConfigPath, nodeConfigPath, parentPath, priorInstallDirName, newInstallDirName, tarballPath)
        if (installOk) {
            /** Do upgrade if necessary */
            if (upgrade) {
                log.emit(s"Upgrade required... upgrade from version $fromKamanja")
                val upgradeOk : Boolean = doMigration(log
                                                    , apiConfigMap
                                                    , nodeConfigPath
                                                    , fromKamanja
                                                    , fromScala
                                                    , toScala
                                                    , parentPath
                                                    , priorInstallDirName
                                                    , newInstallDirName)
                log.emit(s"Upgrade completed...successful?  ${if (upgradeOk) true else false}")
            } else {
                log.emit("Migration not required... new installation was selected")
            }
        } else {
            log.emit("The cluster installation has failed")
            sys.exit(1)
        }
    } else {
        log.emit("The cluster environment is not suitable for an installation or upgrade... look at the prior log entries for more information.  Corrections are needed.")
    }

    log.emit("Processing is Complete!")
    log.close
  }

    def CreateInstallationNames(apiConfigMap: Map[String, String]): (String, String, String) = {

        ("","","")
    }


    def validateClusterEnvironment(log: InstallDriverLog
                                   , apiConfigMap: Map[String, String]
                                   , nodeConfigPath: String
                                   , fromKamanja: String
                                   , fromScala: String
                                   , toScala: String
                                   , upgrade: Boolean
                                   , install: Boolean
                                   , parentPath: String
                                   , priorInstallDirName: String
                                   , newInstallDirName: String
                                   , ips: Array[String]
                                   , ipIdTargPaths: Array[(String, String, String, String)]
                                   , ipPathPairs: Array[(String, String)]): Boolean = {

        val proposedClusterEnvironmentIsSuitable: Boolean = true

        /** Check Java on the cluster */
        /** Check Scala on the cluster */
        /** Check if zookeeper is workable */
        /** Check for suitable kafka environment */
        /** Check the hBase */

        proposedClusterEnvironmentIsSuitable
    }

    def collectNodeInfo(log : InstallDriverLog, clusterId : String, clusterNodes : List[Map[String,Any]], installDir : String)
    : (Array[String], Array[(String,String,String,String)], Array[(String,String)]) = {
        val configDir: String = s"$installDir/config"
        val ipsSet: Set[String] = clusterNodes.map(info => {
            val nodeInfo: Map[String, _] = info.asInstanceOf[Map[String, _]]
            val ipAddr: String = nodeInfo.getOrElse("NodeIpAddr", "_bo_gu_us_node_ip_ad_dr").asInstanceOf[String]
            ipAddr
        }).toSet
        if (ipsSet.contains("_bo_gu_us_node_ip_ad_dr")) {
            log.emit(s"the node ip information for cluster id $clusterId is invalid... aborting")
            sys.exit(1)
        }
        val ips: Array[String] = ipsSet.toSeq.sorted.toArray

        val ipIdTargPathsSet: Set[(String,String,String,String)] = clusterNodes.map(info => {
            val nodeInfo: Map[String, _] = info.asInstanceOf[Map[String, _]]
            val ipAddr: String = nodeInfo.getOrElse("NodeIpAddr", "_bo_gu_us_node_ip_ad_dr").asInstanceOf[String]
            val nodeId : String = nodeInfo.getOrElse("NodeId", "unkn_own_node_id").asInstanceOf[String]
            val roles : List[String] = nodeInfo.getOrElse("Roles", List[String]()).asInstanceOf[List[String]]
            val rolesStr : String = if (roles.size > 0) roles.mkString(",") else "UNKNOWN_ROLES"
            (ipAddr, nodeId, configDir, rolesStr)
        }).toSet
        val ipIdTargPathsReasonable : Boolean = ipIdTargPathsSet.filter(quartet => {
            val (ipAddr, nodeId, _, rolesStr) : (String,String,String,String) = quartet
            (ipAddr ==  "_bo_gu_us_node_ip_ad_dr" || nodeId == "unkn_own_node_id" || rolesStr == "UNKNOWN_ROLES")
        }).size == 0
        if (! ipIdTargPathsReasonable) {
            log.emit(s"the node ip addr, node identifier, and/or node roles are bad for cluster id $clusterId ... aborting")
            sys.exit(1)
        }
        val ipIdTargPaths: Array[(String,String,String,String)] = ipIdTargPathsSet.toSeq.sorted.toArray

        val uniqueNodePaths: Set[String] = clusterNodes.map(info => {
            val nodeInfo: Map[String, _] = info.asInstanceOf[Map[String, _]]
            val ipAddr: String = nodeInfo.getOrElse("NodeIpAddr", "_bo_gu_us_node_ip_ad_dr").asInstanceOf[String]
            s"$ipAddr~$configDir"
        }).toSet
        val ipPathPairs: Array[(String, String)] = uniqueNodePaths.map(itm => (itm.split('~').head, itm.split('~').last)).toSeq.sorted.toArray

        (ips, ipIdTargPaths, ipPathPairs)
    }



  def clusterNodes(ips : Array[String]) : String = {
      val buffer : StringBuilder = new StringBuilder
      ""
  }


  def mapProperties(log : InstallDriverLog, apiConfigPath : String) : Map[String,String] = {

      val mapPropertyLines : List[String] = Source.fromFile(apiConfigPath).mkString.split('\n').toList
      val mapProperties : Map[String,String] = mapPropertyLines.filter( l => (l != null && l.size > 0 && l.contains("="))).map(line => {
          val key : String = line.split('=').head.trim.toLowerCase
          val value : String = line.split('=').last.trim
          (key,value) 
      }).toMap

      mapProperties
  }

  def CreateInstallationNames(log : InstallDriverLog, apiConfigMap : Map[String,String]) : (String, String, String) = {

      val parPath : String = apiConfigMap.getOrElse("ROOT_DIR", "NO ROOT PATH")

      val (parentPath, priorInstallDirName, newInstallDirName) : (String, String, String) = if (parPath == "NO ROOT PATH") {
          log.emit("There is no ROOT_DIR in the supplied api configuration file... Examine that file and correct it... quiting")
          ("","","")
      } else {
          val dateTime : DateTime = new DateTime
          val fmt : DateTimeFormatter  = DateTimeFormat.forPattern("yyyyMMMdd_HHmmss")
          val datestr : String = fmt.print(dateTime);
          (parPath, s"$parPath/priorInstallation$datestr", s"$parPath/newInstallation$datestr")
      }

      (parentPath, priorInstallDirName, newInstallDirName)
  }

  def installCluster(log : InstallDriverLog, apiConfigPath : String, nodeConfigPath : String, parentPath : String, priorInstallDirName : String, newInstallDirName : String, tarballPath : String) : Boolean = {

      val installCmd = Seq("bash", "-c", s"KamanjaClusterInstall.sh  --MetadataAPIConfig $apiConfigPath, --NodeConfigPath $nodeConfigPath, --ParentPath $parentPath, --PriorInstallDirName $priorInstallDirName, --NewInstallDirName $newInstallDirName --TarballPath $tarballPath")
      log.emit(s"KamanjaClusterInstall cmd used: $installCmd")
      val installCmdRc = Process(installCmd).!
      if (installCmdRc != 0) {
        log.emit(s"KamanjaClusterInstall has failed...rc = $installCmdRc")
        log.emit(s"Command used: $installCmd")
      }
      (installCmdRc != 0)
  }

  def doMigration(log : InstallDriverLog
                , apiConfigMap : Map[String,String]
                , nodeConfigPath : String
                , fromKamanja : String
                , fromScala : String
                , toScala : String
                , parentPath : String
                , priorInstallDirName : String
                , newInstallDirName : String) : Boolean = {

      val migrationToBeDone : String = if (fromKamanja == "1.1") "1.1=>1.3" else if (fromKamanja == "1.2") "1.2=>1.3" else "hmmm"
      
      val cfgPath : String = "/tmp/migrateCfg.json"
      val upgradeOk : Boolean = migrationToBeDone match {
          case "1.1=>1.3" => {
              val migrateConfigJSON : String = createMigrationConfig(log
                                                                    , "1.1"
                                                                    , "1_1"
                                                                    , s"$parentPath/dumpdir"
                                                                    , s"$parentPath/$priorInstallDirName"
                                                                    , s"$parentPath/$newInstallDirName")

              writeCfgFile(migrateConfigJSON, cfgPath)
              val migrateCmd = Seq("bash", "-c", s"MigrateManager-1.0 --config $cfgPath")
              log.emit(s"Migrate command used: bash -c MigrateManager-1.0 --config $cfgPath")
              val migrateCmdRc = Process(migrateCmd).!
              val ok : Boolean = if (migrateCmdRc != 0) {
                  log.emit(s"migration has failed has failed...rc = $migrateCmdRc")
                  false
              } else {
                  log.emit("migration was successful")
                  true
              }
              ok
          }
          case "1.2=>1.3" =>  {
              val migrateConfigJSON : String = createMigrationConfig(log
                                                                  , "1.2"
                                                                  , "1_2"
                                                                  , s"$parentPath/dumpdir"
                                                                  , s"$parentPath/$priorInstallDirName"
                                                                  , s"$parentPath/$newInstallDirName")
              writeCfgFile(migrateConfigJSON, cfgPath)
              val migrateCmd = Seq("bash", "-c", s"MigrateManager-1.0 --config $cfgPath")
              log.emit(s"Migrate command used: bash -c MigrateManager-1.0 --config $cfgPath")
              val migrateCmdRc = Process(migrateCmd).!
              val ok : Boolean = if (migrateCmdRc != 0) {
                  log.emit(s"migration has failed has failed...rc = $migrateCmdRc")
                  false
              } else {
                  log.emit("migration was successful")
                  true
              }
              ok
          }
          case _ => false
      }
      upgradeOk
  }

  /**
    *
    * Write the supplied configuration string to the supplied path.
    *
    * @param cfgString some configuration
      @param filePath  the path of the file that will be created.  It should be valid... no checks here.

   */
  private def writeCfgFile(cfgString : String, filePath : String) {
    val file = new File(filePath);
    val bufferedWriter = new BufferedWriter(new FileWriter(file))
    bufferedWriter.write(cfgString)
    bufferedWriter.close
  }
  


  /** Answer a migration configuration json structure that contains the requisite migration info required for one of the migration/upgrade paths.

    * FIXME: This has a hard coded version for the migrationFrom.  Don't we want to substitute that ?????? I am doing it below for now... Pokuri, please fix as appopriate. We need
    * to change the lib version as well I think...

   */
  def createMigrationConfig(log : InstallDriverLog, fromKamanjaVersion : String, libVersion : String, unhandledMetadataDumpDir : String, priorInstallationPath : String, newInstallationPath : String) : String = {

val template : String = """
{
      "clusterConfigFile": "%newInstallationPath%/config/ClusterConfig.json",
      "apiConfigFile": "%newInstallationPath%/config/MetadataAPIConfig.properties",
      "unhandledMetadataDumpDir": "%unhandledMetadataDumpDir%",
      "dataSaveThreshold": 1000,
      "migratingFrom": {
        "version": "%fromKamanjaVersion%",
        "versionInstallPath": "%priorInstallationPath%",
        "implemtedClass": "com.ligadata.Migrate.MigrateFrom_V_1_1",
        "jars": [
          "$newInstallationPath/lib/system/migratebase-1.0.jar",
          "$newInstallationPath/lib/system/migratefrom_v_%libVersion%_2.10-1.0.jar",
          "$priorInstallationPath/bin/KamanjaManager-1.0"
        ]
      },
      "migratingTo": {
        "version": "1.3",
        "versionInstallPath": "%newInstallationPath%",
        "implemtedClass": "com.ligadata.Migrate.MigrateTo_V_1_3",
        "jars": [
          "$newInstallationPath/lib/system/migratebase-1.0.jar",
          "$newInstallationPath/lib/system/migrateto_v_1_3_2.11-1.0.jar",
          "$newInstallationPath/bin/KamanjaManager-1.0"
        ]
      },
      "excludeMetadata": [
        "ModelDef",
        "MessageDef",
        "ContainerDef",
        "ConfigDef",
        "FunctionDef",
        "JarDef",
        "OutputMsgDef"
      ],
      "excludeData": false
}
"""

      val subPairs : Array[(String,String)] = Array[(String,String)]( ("fromKamanjaVersion", fromKamanjaVersion), ("libVersion",libVersion), ("unhandledMetadataDumpDir", unhandledMetadataDumpDir), ("priorInstallationPath", priorInstallationPath), ("newInstallationPath", newInstallationPath))
      val substitutionMap : Map[String,String] = subPairs.toMap
      val varSub = new MapSubstitution(template, substitutionMap)
      varSub.makeSubstitutions
  }





}

class ClusterConfigMap(cfgStr: String, clusterIdOfInterest : String) {

    val clusterMap : Map[String,Any] = getClusterConfigMapOfInterest(cfgStr, clusterIdOfInterest)
    if (clusterMap.size == 0) {
        throw new RuntimeException(s"There is no cluster information for cluster $clusterIdOfInterest")
    }
    val clusterId : String =  getClusterId
    val dataStore : Map[String,Any] = getDataStore
    val zooKeeperInfo : Map[String,Any] = getZooKeeperInfo
    val environmentContext : Map[String,Any] = getEnvironmentContext
    val clusterNodes : List[Map[String,Any]] = getClusterNodes
    val adapters : List[Map[String,Any]] = getAdapters

    def ClusterMap : Map[String,Any] = clusterMap
    def ClusterId : String = clusterId
    def DataStore : Map[String,Any] = dataStore
    def ZooKeeperInfo : Map[String,Any] = zooKeeperInfo
    def EnvironmentContext : Map[String,Any] = environmentContext
    def ClusterNodes : List[Map[String,Any]] = clusterNodes
    def Adapters : List[Map[String,Any]] = adapters

    private def getClusterConfigMapOfInterest(cfgStr: String, clusterIdOfInterest : String): Map[String,Any] = {
        val clusterMap : Map[String,Any] = try {
            // extract config objects
            val map = JsonSerializer.parseEngineConfig(cfgStr)
            // process clusterInfo object if it exists
            val mapOfInterest : Map[String,Any] = if (map.contains("Clusters")) {
                val clusterList : List[_] = map.get("Clusters").get.asInstanceOf[List[_]]
                val clusters = clusterList.length

                val clusterSought : String = clusterIdOfInterest.toLowerCase
                val clusterIdList : List[Any] = clusterList.filter(aCluster => {
                    val cluster : Map[String,Any] =  aCluster.asInstanceOf[Map[String,Any]]
                    val clusterId = cluster.getOrElse("ClusterId", "").toString.trim.toLowerCase
                    (clusterId == clusterSought)
                })

                val clusterOfInterestMap : Map[String,Any] =  if (clusterIdList.size > 0) {
                    clusterIdList.head.asInstanceOf[Map[String, Any]]
                } else {
                    Map[String, Any]()
                }
                clusterOfInterestMap
            } else {
                Map[String,Any]()
            }
            mapOfInterest
        } catch {
            case e: Exception => {
                throw new Exception("Failed to parse clusterconfig", e)
            }
            case e: Throwable => {
                throw new Exception("Failed to parse clusterconfig", e)
            }
        }
        clusterMap
    }

    private def getClusterId: String = {
        val clustId : String = if (clusterMap.size > 0 && clusterMap.contains("ClusterId")) {
            clusterMap("ClusterId").asInstanceOf[String]
        } else {
            ""
        }
        clustId
    }

    private def getDataStore : Map[String,Any] = {
        val dataStoreMap : Map[String,Any] = if (clusterMap.size > 0 && clusterMap.contains("DataStore")) {
            clusterMap("DataStore").asInstanceOf[Map[String,Any]]
        } else {
            Map[String,Any]()
        }
        dataStoreMap
    }

    private def getZooKeeperInfo : Map[String,Any] = {
        val dataStoreMap : Map[String,Any] = if (clusterMap.size > 0 && clusterMap.contains("ZooKeeperInfo")) {
            clusterMap("ZooKeeperInfo").asInstanceOf[Map[String,Any]]
        } else {
            Map[String,Any]()
        }
        dataStoreMap
    }

    private def getEnvironmentContext : Map[String,Any] = {
        val envCtxMap : Map[String,Any] = if (clusterMap.size > 0 && clusterMap.contains("EnvironmentContext")) {
            clusterMap("EnvironmentContext").asInstanceOf[Map[String,Any]]
        } else {
            Map[String,Any]()
        }
        envCtxMap
    }

    private def getClusterNodes : List[Map[String,Any]] = {
        val nodeMapList : List[Map[String,Any]] = if (clusterMap.size > 0 && clusterMap.contains("Nodes")) {
            clusterMap("Nodes").asInstanceOf[List[Map[String,Any]]]
        } else {
            List[Map[String,Any]]()
        }
        nodeMapList
    }

    private def getAdapters : List[Map[String,Any]] = {
        val adapterMapList : List[Map[String,Any]] = if (clusterMap.size > 0 && clusterMap.contains("Adapters")) {
            clusterMap("Adapters").asInstanceOf[List[Map[String,Any]]]
        } else {
            List[Map[String,Any]]()
        }
        adapterMapList
    }

}

class MapSubstitution(template: String, vars: scala.collection.immutable.Map[String, String]) {

    def findAndReplace(m: Matcher)(callback: String => String):String = {
        val sb = new StringBuffer
        while (m.find) {
            val replStr = vars(m.group(1))
            m.appendReplacement(sb, callback(replStr))
        }
        m.appendTail(sb)
        sb.toString
    }

    def makeSubstitutions : String = {
        val m = Pattern.compile("""(%[A-Za-z_.-]+%)""").matcher(template)
        findAndReplace(m){ x => x }
    }

}

