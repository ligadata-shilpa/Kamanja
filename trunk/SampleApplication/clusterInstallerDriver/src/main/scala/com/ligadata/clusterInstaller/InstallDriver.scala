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

import java.util.Properties

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.io.Source
import scala.sys.process._

import java.io._
import java.util.regex.{Matcher, Pattern}

import org.apache.logging.log4j.LogManager
import org.json4s._

import com.ligadata.Serialize.JsonSerializer
import com.ligadata.Migrate.{Migrate, StatusCallback}
import com.ligadata.Utils.Utils

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
  * 2) The migration script requires a JSON file that contains information about the upgrade.  There are substitution symbols ("macros") in the
  * file that are substituted with the appropriate values, either supplied in the script parameters or from one of the configuation files
  * whose path was supplied.  The substitution symbols have the form """({[A-Za-z0-9_.-]+})""" ... that is, some run 1 or more Alphamerics and punctuation
  * enclosed in {} braces.  For example,
  *
  *     "clusterConfigFile": "{NewPackageInstallPath}/config/ClusterConfig.json",
  *
  * See trunk/SampleApplication/clusterInstallerDriver/src/main/resources/MigrateConfigTemplate.json in the github dev repo for an example template.
  */

class InstallDriverLog(val logPath : String) extends StatusCallback {

  val bufferedWriter = new BufferedWriter(new FileWriter(new File(logPath)))
  var isReady : Boolean = true

  /**
    * open the discrete log file if possible.  If open fails (file must not previously exist) false is returned */
    /**
      * Answer if this object can write messages
      *
      * @return
      */
    def ready : Boolean = {
        isReady
    }

    /**
      * Emit the supplied msg to the buffered FileWriter contained in this InstallDriverLog instance.
      *
      * @param msg a message of importance
      */
    def emit(msg : String) : Unit = {
        if (ready) {
            val dateTime : DateTime = new DateTime
            val fmt : DateTimeFormatter  = DateTimeFormat.forPattern("yyyy-MM-dd_HH:mm:ss.SSS")
            val datestr : String = fmt.print(dateTime);

            bufferedWriter.write(msg + "\n")
        }
    }

    /**
      * Close the buffered FileWriter to flush last buffer and close file.
      */
    def close : Unit = {
        bufferedWriter.close
        isReady = false
    }

    /**
      * StatusCallback implementation.  The InstallDriverLog is passed to the migrate component and it will call back
      * here that it deems worth reporting to the install driver log.
      *
      * @param statusText
      */
    def call (statusText: String) : Unit = {
        emit(statusText)
    }
}

object InstallDriver extends App {
    lazy val loggerName = this.getClass.getName
    lazy val logger = LogManager.getLogger(loggerName)

    def usage: String = {
    """
    java -Dlog4j.configurationFile=file:./log4j2.xml -jar <some path> ClusterInstallerDriver_1.3
            --{upgrade|install}
            --apiConfig <MetadataAPIConfig.properties file>
            --clusterConfig <ClusterConig.json file>
            --fromKamanja "1.1"
            [--fromScala "2.10"]
            [--toScala "2.11"]
            --workingDir <workingdirectory>
            --clusterId <id>
            --tarballPath <tarball path>
            --logDir <logDir>
            --migrationTemplate <MigrationTemplate>

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
        --migrationTemplate <MigrationTemplate> the path of the migration template to be used.  This file has several configurable values as well as a number
          of values that are automatically filled with information gleaned from other files in this list
        --logDir for both Unhandled metadata & logs. If this value is not specified, it defaults to /tmp.  It is highly recommended that you
          specify a more permanent path so that the installations you do can be documented in a location not prone to being deleted.

    The ClusterInstallerDriver_1.3 is the cluster installer driver for Kamanja 1.3.  It is capable of installing a new version of 1.3 or given the appropriate arguments,
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
            case "--migrationTemplate" :: value :: tail =>
                nextOption(map ++ Map('migrationTemplate -> value), tail)
            case "--logDir" :: value :: tail =>
                nextOption(map ++ Map('logDir -> value), tail)
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
        val toScala : String = if (options.contains('toScala)) options.apply('toScala) else "" // FIXME: Insist to have this
        val workingDir : String = if (options.contains('workingDir)) options.apply('workingDir) else null
        val upgrade : Boolean = if (options.contains('upgrade)) options.apply('upgrade) == "true" else false
        val install : Boolean = if (options.contains('install)) options.apply('install) == "true" else false
        val migrateTemplate : String = if (options.contains('migrateTemplate)) options.apply('migrateTemplate) else null
        val logDir : String = if (options.contains('logDir)) options.apply('logDir) else "/tmp"

        /** make a log ... Warning... if logDir not supplied, logDir defaults to /tmp */
        val dateTime : DateTime = new DateTime
        val fmt : DateTimeFormatter  = DateTimeFormat.forPattern("yyyy-MM-dd_HH:mm:ss.SSS")
        val datestr : String = fmt.print(dateTime);
        val log : InstallDriverLog = new InstallDriverLog(s"$logDir/InstallDriver.$datestr.log")
        println(s"The installation log file can be found in $logDir/InstallDriver.$datestr.log")

        val confusedIntention : Boolean = (upgrade && install)
        if (confusedIntention) {
            log.emit("You have specified both 'upgrade' and 'install' as your action.  It must be one or the other. Use ")
            log.emit("'upgrade' if you have an existing Kamanja installation and wish to install the new version of the ")
            log.emit("software and upgrade your application metadata to the new system.")
            log.emit("Use 'install' if the desire is to simply create a new installation.  However, if the 'install' ")
            log.emit("detects an existing installation, the installation will fail.")
            log.emit("Try again.")
            log.close
            sys.exit(1)
        }

        val reasonableArguments: Boolean = (
            clusterId != null && clusterId.nonEmpty
            && apiConfigPath != null && apiConfigPath.nonEmpty
            && nodeConfigPath != null && nodeConfigPath.nonEmpty
            && tarballPath != null && tarballPath.nonEmpty
            && fromKamanja != null && fromKamanja.nonEmpty && (fromKamanja == "1.1" || fromKamanja == "1.2")
            && fromScala != null && fromScala.nonEmpty && fromScala == "2.10"
            && toScala != null && toScala.nonEmpty && (toScala == "2.10" || toScala == "2.11")
            && workingDir != null && workingDir.nonEmpty
            && migrateTemplate != null && migrateTemplate.nonEmpty
            && logDir != null && logDir.nonEmpty
            && ! confusedIntention)

        if (! reasonableArguments) {
            println("Your arguments are not satisfactory...Usage:")
            println(usage)
            sys.exit(1)
        }

        // Validate all arguments
        // FIXME: Validate here itself
        var cnt : Int = 0
        val migrationToBeDone : String = if (fromKamanja == "1.1") "1.1=>1.3" else if (fromKamanja == "1.2") "1.2=>1.3" else "hmmm"
        if (migrationToBeDone == "hmmm") {
            log.emit(s"The fromKamanja ($fromKamanja) is not valid with this release... the value must be 1.1 or 1.2")
            cnt += 1
        }

        /** validate the paths before proceeding */
        val apiCfgExists : Int = Seq("ls ", apiConfigPath).!
        val nodeConfigPathExists : Int = Seq("ls ", nodeConfigPath).!
        val tarballPathExists : Int = Seq("ls ", tarballPath).!
        val workingDirExists : Int = Seq("ls ", workingDir).!
        val migrateTemplateExists : Int = Seq("ls ", migrateTemplate).!
        val logDirExists : Int = Seq("ls ", logDir).!

        if (apiCfgExists != 0) {
            log.emit(s"The apiConfigPath ($apiConfigPath) does not exist")
            cnt += 1
        }
        if (nodeConfigPathExists != 0) {
            log.emit(s"The nodeConfigPath ($nodeConfigPath) does not exist")
            cnt += 1
        }
        if (tarballPathExists != 0) {
            log.emit(s"The tarballPath ($tarballPath) does not exist")
            cnt += 1
        }
        if (workingDirExists != 0) {
            log.emit(s"The workingDir ($workingDir) does not exist")
            cnt += 1
        }
        if (migrateTemplateExists != 0) {
            log.emit(s"The migrateTemplate ($migrateTemplate) does not exist")
            cnt += 1
        }
        if (logDirExists != 0) {
            log.emit(s"The logDir ($logDir) does not exist")
            cnt += 1
        }
        if (cnt > 0) {
            log.emit("Please fix your arguments and try again.")
            log.close
            sys.exit(1)
        }

        /** Convert the content of the property file into a map.  If the path is bad, an empty map is returned and processing stops */
        val apiConfigMap : Properties = mapProperties(log, apiConfigPath)
        if (apiConfigMap == null || apiConfigMap.isEmpty) {
            log.emit("The configuration file is messed up... it needs to be lines of key=value pairs")
            log.emit(usage)
            log.close
            sys.exit(1)
        }

        /** Ascertain what the name of the new installation directory will be, what the name of the prior installation would be (post install), and the parent
          * directory in which both of them will live on each cluster node. Should there be no existing installation, the prior installation value will be null. */
        val (parentPath, priorInstallDirName, newInstallDirName) : (String, String, String) = CreateInstallationNames(log, apiConfigMap)

        // FIXME: Note that this check must be done on each cluster node.  This work is implemented in the validateClusterEnvironment method below.
        // Check priorInstallDirName is link or dir. If it is DIR take an action (renaming on all nodes) and LOG the task done.
        // IF priorInstallDirName is link, unlink it. But log the Actual dir & link to make sure for Reverting the installation.

        /** Run the node info extract on the supplied file and garner all of the information needed to conduct the cluster environment validatation */
        val installDir : String = s"$parentPath/$newInstallDirName"

        val clusterConfig : String = Source.fromFile(nodeConfigPath).mkString
        val clusterConfigMap : ClusterConfigMap = new ClusterConfigMap(clusterConfig, clusterId)


        val clusterMap : Map[String,Any] = clusterConfigMap.ClusterMap
        if (clusterMap.isEmpty) {
            log.emit(s"There is no cluster info for the supplied clusterId, $clusterId")
            sys.exit(1)
        }
        /** Create the cluster config map and pull out the top level objects... either Maps or Lists of Maps */
        val clusterIdFromConfig : String = clusterConfigMap.ClusterId
        val dataStore : Map[String,Any] = clusterConfigMap.DataStore
        val zooKeeperInfo : Map[String,Any] = clusterConfigMap.ZooKeeperInfo
        val environmentContext : Map[String,Any] = clusterConfigMap.EnvironmentContext
        val clusterNodes : List[Map[String,Any]] = clusterConfigMap.ClusterNodes
        val adapters : List[Map[String,Any]] = clusterConfigMap.Adapters

        /* Collect the node information needed to valididate the implied cluster environment and serve as basic
         * parameters for the new cluster installation */
        val (ips, ipIdTargPaths, ipPathPairs) : (Array[String], Array[(String,String,String,String)], Array[(String,String)]) =
                collectNodeInfo(log, clusterId, clusterNodes, installDir)

        /** Validate that the proposed installation has the requisite characteristics (scala, java, zookeeper, kafka, and hbase) */
        val shouldUpgrade : Boolean = upgrade
        val proposedClusterEnvironmentIsSuitable  : Boolean = validateClusterEnvironment(log
                                                                                      , apiConfigMap
                                                                                      , nodeConfigPath
                                                                                      , fromKamanja
                                                                                      , fromScala
                                                                                      , toScala
                                                                                      , shouldUpgrade
                                                                                      , parentPath
                                                                                      , priorInstallDirName
                                                                                      , newInstallDirName
                                                                                      , ips, ipIdTargPaths, ipPathPairs)
        if (proposedClusterEnvironmentIsSuitable) { /** if so... */

            /** Install the new installation */
            val nodes : String = ips.mkString(",")
            log.emit(s"Begin cluster installation... installation found on each cluster node(any {$nodes}) at $installDir")
            val installOk : Boolean = installCluster(log
                , apiConfigPath
                , nodeConfigPath
                , parentPath
                , priorInstallDirName
                , newInstallDirName
                , tarballPath
                , ips
                , ipIdTargPaths
                , ipPathPairs)
            if (installOk) {
                /** Do upgrade if necessary */
                if (upgrade) {
                    log.emit(s"Upgrade required... upgrade from version $fromKamanja")
                    val upgradeOk : Boolean = doMigration(log
                                                        , apiConfigPath
                                                        , apiConfigMap
                                                        , nodeConfigPath
                                                        , migrateTemplate
                                                        , logDir
                                                        , fromKamanja
                                                        , fromScala
                                                        , toScala
                                                        , parentPath
                                                        , priorInstallDirName
                                                        , newInstallDirName)
                    log.emit(s"Upgrade completed...successful?  ${if (upgradeOk) "yes!" else "false!"}")
                    if (! upgradeOk) {
                        log.emit(s"The parameters for the migration are incorrect... aborting installation")
                        log.close
                        sys.exit(1)
                    }
                } else {
                    log.emit("Migration not required... new installation was selected")
                }
            } else {
                log.emit("The cluster installation has failed")
                sys.exit(1)
            }
        } else {
            log.emit("The cluster environment is not suitable for an installation or upgrade... look at the prior log entries for more information.  Corrections are needed.")
            log.close
            sys.exit(1)
        }

        log.emit("Processing is Complete!")
        log.close
    }

    /**
      * Here are input & outputs from the new function which calls the script and get the values
      * Input:
      * ROOT_DIR_PATH // To get the actual DIR name, if it is soft link otherwise it returns as it is
      * Scriptpath // Path of the script to get all the Components information
      * GetComponentVersionJarPath // FAT JAR to copy to client node and execute that script
      * RemoteNodeIp // Remote Node IP to do scp & ssh
      * ResultsFile // Results file will be writen here and parsed to Array[ComponentInfo]

      * Output: (Array[ComponentInfo], physicalRootDir)
      * where ComponentInfo is
      * case class ComponentInfo(version: String, status: String, errorMessage: String, componentName: String, invocationNode: String);
      *

      * @param log the InstallDriverLog instance that records important processing events and information needed if mal configurations detected
      * @param apiConfigMap the metadata api properties (including the ROOT_DIR
      * @param nodeConfigPath the node configuration file for the cluster
      * @param fromKamanja the version of an existing Kamanja installation that is present on this cluster (if any)
      * @param fromScala the version of Scala used on the existing cluster (if any)
      * @param toScala the version desired of Scala desired on the new installation
      * @param upgrade the user's intent that this is to be an upgrade else it is new install
      * @param parentPath the directory in which both the new and (if upgrading) prior installation directory will be installed
      * @param priorInstallDirName the name of the prior installation name
      * @param newInstallDirName the new installation name
      * @param ips the ip addresses of the nodes participating in this cluster
      * @param ipIdTargPaths a quartet (ip, node id, install path, node roles)
      * @param ipPathPairs an array of pair (ip, install path)
      * @return true if the environment is acceptable
      *
      *         NOTE: Some of the parameters may be eliminated if they can be conclusively ruled out as being
      *         worthy of inspection or testing for this cluster environment worthiness check.
      */

    def validateClusterEnvironment(log: InstallDriverLog
                                   , apiConfigMap: Properties
                                   , nodeConfigPath: String
                                   , fromKamanja: String
                                   , fromScala: String
                                   , toScala: String
                                   , upgrade: Boolean
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
            log.close
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
            log.close
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

    /**
      * Produce a properties map by ingesting the contents of the supplied file. If the file is invalid or an error occurs,
      * the log is updated with the error message and processing stops.
      *
      * @param log
      * @param apiConfigPath
      * @return a Properties instance
      */
    def mapProperties(log : InstallDriverLog, apiConfigPath : String) : Properties = {

        val (properties, errorDescr) : (Properties, String) = Utils.loadConfiguration(apiConfigPath, true)

        if (errorDescr != null) {
            log.emit(s"The apiConfigPath properties path ($apiConfigPath) could not produce a valid set of properties...aborting")
            log.close
            sys.exit(1)
        }

        properties
    }


    def CreateInstallationNames(log : InstallDriverLog, apiConfigMap : Properties) : (String, String, String) = {

        val parPath : String = if (apiConfigMap.getProperty("ROOT_DIR") != null) apiConfigMap.getProperty("ROOT_DIR") else "NO ROOT PATH"
        /** check if this a link */

        val (parentPath, priorInstallDirName, newInstallDirName) : (String, String, String) = if (parPath == "NO ROOT PATH") {
            log.emit("There is no ROOT_DIR in the supplied api configuration file... Examine that file and correct it... installation aborted")
            log.close
            sys.exit(1)
            ("","","")
        } else {
            val dateTime : DateTime = new DateTime
            val fmt : DateTimeFormatter  = DateTimeFormat.forPattern("yyyyMMMdd_HHmmss")
            val datestr : String = fmt.print(dateTime);
            /*
            // ROOT_DIR/parPath as /apps/KamanjaInstall can be link or dir. If it has link that is prior installation. Otherwise create as below

            /apps/KamanjaInstall =>

            /apps/KamanjaInstall_pre_<datetime> if  ROOT_DIR/parPath is DIR

            /apps/KamanjaInstall_<version>_<datetime>
            */
            (parPath, s"$parPath/priorInstallation$datestr", s"$parPath/newInstallation$datestr")
        }

        (parentPath, priorInstallDirName, newInstallDirName)
    }

    /**
      * Call the KamanjaClusterInstall with the following parameters that it needs to install the software on the cluster.
      * Important events encountered during the installation are added to the InstallDriverLog instance.
      *
      * @param log the InstallDriverLog that tracks progress and important events of this installation
      * @param apiConfigPath the api config that contains seminal information about the cluster installation
      * @param nodeConfigPath the node config that contains the cluster description used for the installation
      * @param parentPath the location of the installation directories on each node
      * @param priorInstallDirName the name of the directory to be used for a prior installation that is being upgraded (if appropriate)
      * @param newInstallDirName the new installation directory name that will live in parentPath
      * @param tarballPath the local tarball path that contains the kamanja installation
      * @param ips a file path that contains the unique ip addresses for each node in the cluster
      * @param ipIdTargPaths  a file path that contains the ip address, node id, target dir path and roles
      * @param ipPathPairs a file containing the unique ip addresses and path pairs
      * @return true if the installation succeeded.
      */
  def installCluster(log : InstallDriverLog
                     , apiConfigPath : String
                     , nodeConfigPath : String
                     , parentPath : String
                     , priorInstallDirName : String
                     , newInstallDirName : String
                     , tarballPath : String
                     , ips: Array[String]
                     , ipIdTargPaths: Array[(String,String,String,String)]
                     , ipPathPairs: Array[(String,String)]) : Boolean = {

        // Check for KamanjaClusterInstall.sh existance. And see whether KamanjaClusterInstall.sh has all error handling or not.
        val scriptExitsCheck : Seq[String] = Seq("bash", "-c", s"KamanjaClusterInstall.sh")
        val scriptExistsCmdRc = Process(scriptExitsCheck).!
        if (scriptExistsCmdRc != 0) {
            log.emit(s"KamanjaClusterInstall script is not installed... it must be on your PATH... rc = $scriptExistsCmdRc")
            log.emit("Installation is aborted. Consult the log file (${log.logPath}) for details.")
            log.close
            sys.exit(1)
        }

        val installCmd : Seq[String]  = Seq("bash", "-c", s"KamanjaClusterInstall.sh  --MetadataAPIConfig $apiConfigPath, --NodeConfigPath $nodeConfigPath, --ParentPath $parentPath, --PriorInstallDirName $priorInstallDirName, --NewInstallDirName $newInstallDirName --TarballPath $tarballPath --ipAddrs $ips --ipIdTargPaths $ipIdTargPaths --ipPathPairs $ipPathPairs")
        log.emit(s"KamanjaClusterInstall cmd used: $installCmd")
        val installCmdRc : Int = (installCmd #> new File("/tmp/__install_results_")).!
        val installCmdResults : String = Source.fromFile("/tmp/__install_results_").mkString
        if (installCmdRc != 0) {
            log.emit(s"KamanjaClusterInstall has failed...rc = $installCmdRc")
            log.emit(s"Command used: $installCmd")
            log.emit(s"Command report:\n$installCmdResults")
            log.emit(s"Installation is aborted. Consult the log file (${log.logPath}) for details.")
            log.close
            sys.exit(1)
        }
        log.emit(s"New installation command result report:\n$installCmdResults")

        (installCmdRc == 0)
    }

    /**
      *  runFromJsonConfigString(jsonConfigString : String) : Int
      *
      * @param log InstallDriverLog instance that records installation process events
      * @param apiConfigFile the local path location of the metadata api configuration file
      * @param apiConfigMap a Properties file version instance of that fil's content
      * @param nodeConfigPath the cluster nodes configuration file
      * @param migrateConfigFilePath the path to the migrateTemplate
      * @param unhandledMetadataDumpDir the location where logs and unhandledMetadata is dumped
      * @param fromKamanja which Kamanja is being upgraded (e.g., 1.1 or 1.2)
      * @param fromScala the Scala for the existing implementation (2.10)
      * @param toScala the Scala for the new installation (either 2.10 or 2.11)
      * @param parentPath the directory that will contain the new and prior installation
      * @param priorInstallDirName the physical name of the prior installation (after installation completes)
      * @param newInstallDirName the physical name of the new installation
      * @return
      */
  def doMigration(log : InstallDriverLog
                  , apiConfigFile : String
                  , apiConfigMap : Properties
                  , nodeConfigPath : String
                  , migrateConfigFilePath : String
                  , unhandledMetadataDumpDir : String
                  , fromKamanja : String
                  , fromScala : String
                  , toScala : String
                  , parentPath : String
                  , priorInstallDirName : String
                  , newInstallDirName : String) : Boolean = {

      val migrationToBeDone : String = if (fromKamanja == "1.1") "1.1=>1.3" else if (fromKamanja == "1.2") "1.2=>1.3" else "hmmm"
      
      val upgradeOk : Boolean = migrationToBeDone match {
          case "1.1=>1.3" => {
              val kamanjaFromVersion : String = "1.1"
              val kamanjaFromVersionWithUnderscore : String = "1_1"
              val migrateConfigJSON : String = createMigrationConfig(log
                                                                  , migrateConfigFilePath
                                                                  , nodeConfigPath
                                                                  , apiConfigFile
                                                                  , kamanjaFromVersion
                                                                  , kamanjaFromVersionWithUnderscore
                                                                  , newInstallDirName
                                                                  , priorInstallDirName
                                                                  , fromScala
                                                                  , toScala
                                                                  , unhandledMetadataDumpDir
                                                              )
             val migrateObj : Migrate = new Migrate()
             migrateObj.registerStatusCallback(log)
             val rc : Int = migrateObj.runFromJsonConfigString(migrateConfigJSON)
             (rc == 0)
          }
          case "1.2=>1.3" =>  {
              val kamanjaFromVersion : String = "1.1"
              val kamanjaFromVersionWithUnderscore : String = "1_1"
              val migrateConfigJSON : String = createMigrationConfig(log
                  , migrateConfigFilePath
                  , nodeConfigPath
                  , apiConfigFile
                  , kamanjaFromVersion
                  , kamanjaFromVersionWithUnderscore
                  , newInstallDirName
                  , priorInstallDirName
                  , fromScala
                  , toScala
                  , unhandledMetadataDumpDir
              )
              val migrateObj : Migrate = new Migrate()
              migrateObj.registerStatusCallback(log)
              val rc : Int = migrateObj.runFromJsonConfigString(migrateConfigJSON)
              (rc == 0)
          }
          case _ => {
              log.emit("The 'fromKamanja' parameter is incorrect... this needs to be fixed.  The value can only be '1.1' or '1.2' for the '1.3' upgrade")
              false
          }
      }
      if (! upgradeOk) {
          log.emit(s"The upgrade has failed.  Please consult the log (${log.logPath}) for guidance as to how to recover from this.")
          log.close
          sys.exit(1)
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
  



    /**
      * Answer a migration configuration json structure that contains the requisite migration info required for one of the migration/upgrade paths.
      *
      * @param log
      * @param migrateConfigFilePath the local file system path where the template file is found
      * @param clusterConfigFile substitution value
      * @param apiConfigFile substitution value
      * @param kamanjaFromVersion substitution value
      * @param kamanjaFromVersionWithUnderscore substitution value
      * @param newPackageInstallPath substitution value
      * @param oldPackageInstallPath substitution value
      * @param scalaFromVersion substitution value
      * @param scalaToVersion substitution value
      * @param unhandledMetadataDumpDir substitution value
      * @return filled in template with supplied values.
      */
  def createMigrationConfig(log : InstallDriverLog
                            , migrateConfigFilePath : String
                            , clusterConfigFile : String
                            , apiConfigFile : String
                            , kamanjaFromVersion : String
                            , kamanjaFromVersionWithUnderscore : String
                            , newPackageInstallPath : String
                            , oldPackageInstallPath : String
                            , scalaFromVersion : String
                            , scalaToVersion : String
                            , unhandledMetadataDumpDir : String
                           ) : String = {

      val template : String = Source.fromFile(migrateConfigFilePath).mkString

      /**

        * Current Macro names

        * ClusterConfigFile
        * ApiConfigFile
        * KamanjaFromVersion
        * KamanjaFromVersionWithUnderscore
        * NewPackageInstallPath
        * OldPackageInstallPath
        * ScalaFromVersion
        * ScalaToVersion
        * UnhandledMetadataDumpDir

        */

      val subPairs = Map[String,String]( "ClusterConfigFile" -> clusterConfigFile
          , "ApiConfigFile" -> apiConfigFile
          , "KamanjaFromVersion" -> kamanjaFromVersion
          , "KamanjaFromVersionWithUnderscore" -> kamanjaFromVersionWithUnderscore
          , "NewPackageInstallPath" -> newPackageInstallPath
          , "OldPackageInstallPath" -> oldPackageInstallPath
          , "ScalaFromVersion" -> scalaFromVersion
          , "ScalaToVersion" -> scalaToVersion
          , "UnhandledMetadataDumpDir" -> unhandledMetadataDumpDir)

      val substitutionMap : Map[String,String] = subPairs.toMap
      val varSub = new MapSubstitution(template, substitutionMap)
      val substitutedTemplate : String = varSub.makeSubstitutions
      substitutedTemplate
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
        val m = Pattern.compile("""({[A-Za-z0-9_.-]+})""").matcher(template)
        findAndReplace(m){ x => x }
    }

}

