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

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.sys.process._
import sys.process._

import java.io._
import java.util.regex.{Matcher, Pattern}

import org.apache.logging.log4j.LogManager
import org.json4s._

import com.ligadata.Serialize.JsonSerializer
import com.ligadata.Migrate.{Migrate, StatusCallback}
import com.ligadata.Utils.Utils

import org.json4s.jackson.Serialization

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
  * "clusterConfigFile": "{NewPackageInstallPath}/config/ClusterConfig.json",
  *
  * See trunk/SampleApplication/clusterInstallerDriver/src/main/resources/MigrateConfigTemplate.json in the github dev repo for an example template.
  */

class InstallDriverLog(val logPath: String) extends StatusCallback {

  val bufferedWriter = new BufferedWriter(new FileWriter(new File(logPath)))
  var isReady: Boolean = true

  /**
    * open the discrete log file if possible.  If open fails (file must not previously exist) false is returned */
  /**
    * Answer if this object can write messages
    *
    * @return
    */
  def ready: Boolean = {
    isReady
  }

  /**
    * Emit the supplied msg to the buffered FileWriter contained in this InstallDriverLog instance.
    *
    * @param msg a message of importance
    */
  def emit(msg: String): Unit = {
    if (ready) {
      val dateTime: DateTime = new DateTime
      val fmt: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd_HH:mm:ss.SSS")
      val datestr: String = fmt.print(dateTime);

      bufferedWriter.write(msg + "\n")
      bufferedWriter.flush();
    }
  }

  /**
    * Close the buffered FileWriter to flush last buffer and close file.
    */
  def close: Unit = {
    bufferedWriter.close
    isReady = false
  }

  /**
    * StatusCallback implementation.  The InstallDriverLog is passed to the migrate component and it will call back
    * here that it deems worth reporting to the install driver log.
    *
    * @param statusText
    */
  def call(statusText: String): Unit = {
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
            --componentVersionScriptAbsolutePath <check component invocation script path>
            --componentVersionJarAbsolutePath <check component jar path>

    where
        --upgrade explicitly specifies that the intent to upgrade an existing cluster installation with the latest release.
        --install explicitly specifies that the intent that this is to be a new installation.  If there is an existing implementation and
            the --install option is given, the installation will fail.  Similarly, if there is NO existing implementation
            and the --upgrade option has been given, the upgrade will fail. If both --upgrade and --install are given, the install fails.
        --apiConfig <MetadataAPIConfig.properties file> specifies the path of the properties file that (among others) specifies the location
            of the installation on each cluster node given in the cluster configuration.  Note that all installations exist in the same
            location on all nodes.
        --clusterConfig <ClusterConig.json file> gives the file that describes the node information for the cluster, including the IP address of each node, et al.
        [--fromKamanja] optional for install but required for update..."N.N" where "N.N" can be either "1.1" or "1.2"
        --workingDir <workingdirectory> a CRUD directory path that should be addressable on every node specified in the cluster configuration
            file as well as on the machine that this program is executing.  It is used by this program and scripts it invokes to create
            intermediate files used during the installation process.
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
        --componentVersionScriptAbsolutePath the current path of the script that will fetch component information for each cluster node
        --componentVersionJarAbsolutePath the GetComponent jar that will be installed on each cluster node and called remotely by the supplied
          componentVersionScript (i.e., from the machine executing the cluster installer driver).

    The ClusterInstallerDriver_1.3 is the cluster installer driver for Kamanja 1.3.  It is capable of installing a new version of 1.3 or given the appropriate arguments,
    installing a new version of Kamanja 1.3 *and* upgrading a 1.1 or 1.2 installation to the 1.3 version.

    A log of the installation and optional upgrade is collected in a log file.  This log file is automatically generated and will be found in the
    workingDir you supply to the installer driver program.  The name will be of the form: InstallDriver.yyyyMMdd_HHmmss.log (e.g.,
    InstallDriver.20160201_231101.log)  Should issues be encountered (missing components, connectivity issues, etc.) this log should be
    consulted as to how to proceed.  Using the content of the log as a guide, the administrator will see what fixes must be made to their
    environment to push on and complete the install.  It will also be the guide for backing off/abandoning an upgrade so a prior Kamanja cluster
    can be restored.

    """
  }

  override def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("Usage: \n");
      println(usage);
      sys.exit(1)
    }

    // locate the clusterInstallerDriver app ... need its working directory to refer to others... this function
    // returns this form:  file:/tmp/drdigital/KamanjaInstall-1.3.2_2.11/bin/clusterInstallerDriver-1.0

    val thisFatJarsLocationAbs :  String  = getClass().getProtectionDomain().getCodeSource().getLocation().toExternalForm()
    val pathSwizzlePossible : Boolean = (thisFatJarsLocationAbs.contains(':') && thisFatJarsLocationAbs.contains('/'))
    if (! pathSwizzlePossible) {
        throw new RuntimeException("unable to determine the current path for clusterInstallerDriver executable... \nFoundLocation:" + thisFatJarsLocationAbs)
    }

      /** Obtain location of the clusterInstallerDriver fat jar.  Its directory contains the scripts we use to
        * obtain component info for the env check and the lower level cluster install script that actually does the
        * install.
        */
    val clusterInstallerDriversLocation : String = thisFatJarsLocationAbs.split(':').tail.mkString(":").split('/').dropRight(1).mkString("/")

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
        case "--migrateTemplate" :: value :: tail =>
          nextOption(map ++ Map('migrateTemplate -> value), tail)
        case "--componentVersionScriptAbsolutePath" :: value :: tail =>
          nextOption(map ++ Map('componentVersionScriptAbsolutePath -> value), tail)
        case "--componentVersionJarAbsolutePath" :: value :: tail =>
          nextOption(map ++ Map('componentVersionJarAbsolutePath -> value), tail)
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

    val clusterId: String = if (options.contains('clusterId)) options.apply('clusterId) else null
    val apiConfigPath: String = if (options.contains('apiConfig)) options.apply('apiConfig) else null
    val nodeConfigPath: String = if (options.contains('clusterConfig)) options.apply('clusterConfig) else null
    val tarballPath: String = if (options.contains('tarballPath)) options.apply('tarballPath) else null
    val fromKamanja: String = if (options.contains('fromKamanja)) options.apply('fromKamanja) else null
    val fromScala: String = if (options.contains('fromScala)) options.apply('fromScala) else "2.10"
    val toScala: String = if (options.contains('toScala)) options.apply('toScala) else "" // FIXME: Insist to have this
    val workingDir: String = if (options.contains('workingDir)) options.apply('workingDir) else null
    val upgrade: Boolean = if (options.contains('upgrade)) options.apply('upgrade) == "true" else false
    val install: Boolean = if (options.contains('install)) options.apply('install) == "true" else false
    val migrateTemplate: String = if (options.contains('migrateTemplate)) options.apply('migrateTemplate) else null
    val logDir: String = if (options.contains('logDir)) options.apply('logDir) else "/tmp"
    val componentVersionScriptAbsolutePath: String = if (options.contains('componentVersionScriptAbsolutePath)) options.apply('componentVersionScriptAbsolutePath) else null
    val componentVersionJarAbsolutePath: String = if (options.contains('componentVersionJarAbsolutePath)) options.apply('componentVersionJarAbsolutePath) else null

    val toKamanja: String = "1.3"

    /** make a log ... Warning... if logDir not supplied, logDir defaults to /tmp */
    val dateTime: DateTime = new DateTime
    val flfmt: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd_HHmmss")
    val datestr: String = flfmt.print(dateTime);
    val log: InstallDriverLog = new InstallDriverLog(s"$logDir/InstallDriver.$datestr.log")
    println(s"The installation log file can be found in $logDir/InstallDriver.$datestr.log")

    val confusedIntention: Boolean = (upgrade && install) || (! upgrade && ! install)
    if (confusedIntention) {
        println
        println("You have specified both 'upgrade' and 'install' as your action OR NO ACTION.  It must be one or the other. Use ")
        println("'upgrade' if you have an existing Kamanja installation and wish to install the new version of the ")
        println("software and upgrade your application metadata to the new system.")
        println("Use 'install' if the desire is to simply create a new installation.  However, if the 'install' ")
        println("detects an existing installation, the installation will fail.")
        println("Try again.")
        println

        log.emit("You have specified both 'upgrade' and 'install' as your action OR NO ACTION was given.  It must be one or ")
        log.emit("the other. Use 'upgrade' if you have an existing Kamanja installation and wish to install the new version of the ")
        log.emit("software and upgrade your application metadata to the new system.")
        log.emit("Use 'install' if the desire is to simply create a new installation.  However, if the 'install' ")
        log.emit("detects an existing installation, the installation will fail.")
        log.emit("Try again.")
        log.emit("Usage:")
        log.emit(usage)
        log.close

        println("Usage:")
        println(usage)
      sys.exit(1)
    }

    val clusterIdOk: Boolean = clusterId != null && clusterId.nonEmpty
    val apiConfigPathOk: Boolean = apiConfigPath != null && apiConfigPath.nonEmpty
    val tarballPathOk: Boolean = tarballPath != null && tarballPath.nonEmpty
    val fromKamanjaOk: Boolean = (upgrade && fromKamanja != null && fromKamanja.nonEmpty && (fromKamanja == "1.1" || fromKamanja == "1.2")) || install
    val fromScalaOk: Boolean = (upgrade && fromScala != null && fromScala.nonEmpty && (fromScala == "2.10" || fromKamanja == "2.11")) || install
    val toScalaOk: Boolean = (upgrade && toScala != null && toScala.nonEmpty && (toScala == "2.10" || toScala == "2.11")) || install
    val workingDirOk: Boolean = workingDir != null && workingDir.nonEmpty
    val migrateTemplateOk: Boolean = (upgrade && migrateTemplate != null && migrateTemplate.nonEmpty) || install
    val logDirOk: Boolean = logDir != null && logDir.nonEmpty
    val componentVersionScriptAbsolutePathOk: Boolean = componentVersionScriptAbsolutePath != null && componentVersionScriptAbsolutePath.nonEmpty
    val componentVersionJarAbsolutePathOk: Boolean = componentVersionJarAbsolutePath != null && componentVersionJarAbsolutePath.nonEmpty
    val reasonableArguments: Boolean =
      (clusterIdOk
        && apiConfigPathOk
        && tarballPathOk
        && fromKamanjaOk
        && fromScalaOk
        && toScalaOk
        && workingDirOk
        && migrateTemplateOk
        && logDirOk
        && componentVersionScriptAbsolutePathOk
        && componentVersionJarAbsolutePathOk
        )

    if (!reasonableArguments) {
        val installOrUpgrade : String = if (upgrade) "your upgrade" else "your install"
        println(s"One or more arguments for $installOrUpgrade are not set or have bad values...")
        if (!clusterIdOk) println("\t--clusterId <id matching one in --clusterConfig path>")
        if (!apiConfigPathOk) println("\t--clusterConfig <path to valid cluster config file containing cluster id to install>")
        if (!apiConfigPathOk) println("\t--apiConfigPath <path to the metadata api properties file that contains the ROOT_DIR property location>")
        if (!tarballPathOk) println("\t--tarballPath <location of the prepared 1.3 installation tarball to be installed>")
        if (!fromKamanjaOk) println("\t--fromKamanja <the prior installation version being upgraded... either '1.1' or '1.2'>")
        if (!fromScalaOk) println("\t--fromScala <either scala version '2.10' or '2.11'")
        if (!migrateTemplateOk) println("\t--migrateTemplate <path to the migration configuration template file to be used for the upgrade>")
        if (!logDirOk) println("\t--logDir <the directory path where the Cluster logs (InstallDriver.yyyyMMdd_HHmmss.log) is to be written ")
        if (!componentVersionScriptAbsolutePathOk) println("\t--componentVersionScriptAbsolutePath <the path location where the component version script is found")
        if (!componentVersionJarAbsolutePathOk) println("\t--componentVersionJarAbsolutePath <the location of the component check program is found>")
        println("Usage:")
        println(usage)
        sys.exit(1)
    }

    // Validate all arguments
    // FIXME: Validate here itself
    var cnt: Int = 0
    val migrationToBeDone: String = if (fromKamanja == "1.1") "1.1=>1.3" else if (fromKamanja == "1.2") "1.2=>1.3" else "hmmm"
    if (migrationToBeDone == "hmmm") {
      log.emit(s"The fromKamanja ($fromKamanja) is not valid with this release... the value must be 1.1 or 1.2")
      cnt += 1
    }

    /** validate the paths before proceeding */
    val apiCfgExists: Boolean = new File(apiConfigPath).exists
    val nodeConfigPathExists: Boolean = new File(nodeConfigPath).exists
    val tarballPathExists: Boolean = new File(tarballPath).exists
    val workingDirExists: Boolean = new File(workingDir).exists
    val migrateTemplateExists: Boolean = new File(migrateTemplate).exists
    val logDirExists: Boolean = new File(logDir).exists
    val componentVersionScriptAbsolutePathExists: Boolean = new File(componentVersionScriptAbsolutePath).exists
    val componentVersionJarAbsolutePathExists: Boolean = new File(componentVersionJarAbsolutePath).exists

    if (!apiCfgExists) {
      log.emit(s"The apiConfigPath ($apiConfigPath) does not exist")
      cnt += 1
    }
    if (!nodeConfigPathExists) {
      log.emit(s"The nodeConfigPath ($nodeConfigPath) does not exist")
      cnt += 1
    }
    if (!tarballPathExists) {
      log.emit(s"The tarballPath ($tarballPath) does not exist")
      cnt += 1
    }
    if (!workingDirExists) {
      log.emit(s"The workingDir ($workingDir) does not exist")
      cnt += 1
    }
    if (!migrateTemplateExists) {
      log.emit(s"The migrateTemplate ($migrateTemplate) does not exist")
      cnt += 1
    }
    if (!logDirExists) {
      log.emit(s"The logDir ($logDir) does not exist")
      cnt += 1
    }
    if (!componentVersionScriptAbsolutePathExists) {
      log.emit(s"The componentVersionScriptAbsolutePath ($componentVersionScriptAbsolutePath) does not exist")
      cnt += 1
    }
    if (!componentVersionJarAbsolutePathExists) {
      log.emit(s"The componentVersionJarAbsolutePath ($componentVersionJarAbsolutePath) does not exist")
      cnt += 1
    }
    if (cnt > 0) {
      log.emit("Please fix your arguments and try again.")
      log.close
      sys.exit(1)
    }

    /** Convert the content of the property file into a map.  If the path is bad, an empty map is returned and processing stops */
    val apiConfigMap: Properties = mapProperties(log, apiConfigPath)
    if (apiConfigMap == null || apiConfigMap.isEmpty) {
      log.emit("The configuration file is messed up... it needs to be lines of key=value pairs")
      log.emit(usage)
      log.close
      sys.exit(1)
    }

    /** Ascertain what the name of the new installation directory will be, what the name of the prior installation would be
      * (post install), and the parent directory in which both of them will live on each cluster node. Should there be no
      * existing installation, the prior installation value will be null. */
    val rootDirPath: String = apiConfigMap.getProperty("root_dir") /** use root dir value for the base name */
    val (parentPath, priorInstallDirName, newInstallDirName): (String, String, String) = CreateInstallationNames(log
        , rootDirPath
        , fromKamanja
        , toKamanja)

    // FIXME: Note that this check must be done on each cluster node.  This work is implemented in the validateClusterEnvironment method below.
    // Check priorInstallDirName is link or dir. If it is DIR take an action (renaming on all nodes) and LOG the task done.
    // IF priorInstallDirName is link, unlink it. But log the Actual dir & link to make sure for Reverting the installation.

    /** Run the node info extract on the supplied file and garner all of the information needed to conduct the cluster environment validatation */
    val installDir: String = s"$parentPath/$newInstallDirName"

    val clusterConfig: String = Source.fromFile(nodeConfigPath).mkString
    val clusterConfigMap: ClusterConfigMap = new ClusterConfigMap(clusterConfig, clusterId)


    val clusterMap: Map[String, Any] = clusterConfigMap.ClusterMap
    if (clusterMap.isEmpty) {
      log.emit(s"There is no cluster info for the supplied clusterId, $clusterId")
      sys.exit(1)
    }
    /** Create the cluster config map and pull out the top level objects... either Maps or Lists of Maps */
    val clusterIdFromConfig: String = clusterConfigMap.ClusterId
    val dataStore: Map[String, Any] = clusterConfigMap.DataStore
    val zooKeeperInfo: Map[String, Any] = clusterConfigMap.ZooKeeperInfo
    val environmentContext: Map[String, Any] = clusterConfigMap.EnvironmentContext
    val clusterNodes: List[Map[String, Any]] = clusterConfigMap.ClusterNodes
    val adapters: List[Map[String, Any]] = clusterConfigMap.Adapters

    /* Collect the node information needed to valididate the implied cluster environment and serve as basic
     * parameters for the new cluster installation */
    val (ips, ipIdTargPaths, ipPathPairs): (Array[String], Array[(String, String, String, String)], Array[(String, String)]) =
      collectNodeInfo(log, clusterId, clusterNodes, installDir)

    /** Validate that the proposed installation has the requisite characteristics (scala, java, zookeeper, kafka, and hbase) */
    val shouldUpgrade: Boolean = upgrade
    val (proposedClusterEnvironmentIsSuitable, physicalRootDir): (Boolean, String) =
      validateClusterEnvironment(log
        , clusterInstallerDriversLocation
        , clusterConfigMap
        , componentVersionScriptAbsolutePath
        , componentVersionJarAbsolutePath
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
    if (proposedClusterEnvironmentIsSuitable) {
      /** if so... */

      /** Install the new installation */
      val nodes: String = ips.mkString(",")
      log.emit(s"Begin cluster installation... installation found on each cluster node(any {$nodes}) at $installDir")
      val installOk: Boolean = installCluster(log
        , clusterInstallerDriversLocation
        , physicalRootDir
        , apiConfigPath
        , nodeConfigPath
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
          val upgradeOk: Boolean = doMigration(log
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
          if (!upgradeOk) {
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
    *
    * The scala version on each cluster node is tested against the toScala version supplied as a parameter value here.
    * The java must be 1.7x or 1.8x.
    * The zk, kafka and hbase are tested against their status.  The test is assumed to be that the client protocols can
    * access their respective servers.
    * There is a test of the physical root dirs returned.  They should all be the same path (or so I think).
    *
    * @param log                                the InstallDriverLog instance that records important processing events and information needed if mal configurations detected
    * @param clusterInstallerDriversLocation    the location of the script that calls GetComponent
    * @param componentVersionScriptAbsolutePath the GetComponent script that invokes the GetComponent program on each node
    * @param componentVersionJarAbsolutePath    the GetComponent tarball that is shipped to each cluster node
    * @param apiConfigMap                       the metadata api properties (including the ROOT_DIR
    * @param nodeConfigPath                     the node configuration file for the cluster
    * @param fromKamanja                        the version of an existing Kamanja installation that is present on this cluster (if any)
    * @param fromScala                          the version of Scala used on the existing cluster (if any)
    * @param toScala                            the version desired of Scala desired on the new installation
    * @param upgrade                            the user's intent that this is to be an upgrade else it is new install
    * @param priorInstallDirName                the name of the prior installation name
    * @param newInstallDirName                  the new installation name
    * @param ips                                the ip addresses of the nodes participating in this cluster
    * @param ipIdTargPaths                      a quartet (ip, node id, install path, node roles)
    * @param ipPathPairs                        an array of pair (ip, install path)
    * @return true if the environment is acceptable
    *
    *         NOTE: Some of the parameters may be eliminated if they can be conclusively ruled out as being
    *         worthy of inspection or testing for this cluster environment worthiness check.
    */

  def validateClusterEnvironment(log: InstallDriverLog
                                 , clusterInstallerDriversLocation : String
                                 , clusterConfigMap: ClusterConfigMap
                                 , componentVersionScriptAbsolutePath: String
                                 , componentVersionJarAbsolutePath: String
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
                                 , ipPathPairs: Array[(String, String)]): (Boolean, String) = {

    var phyDirLast: String = null
    val rootDirPath: String = apiConfigMap.getProperty("root_dir")

    val (proposedClusterEnvironmentIsSuitable): Boolean = try {
      val hbaseConnections: String = clusterConfigMap.DataStoreConnections //
      val kafkaConnections: String = clusterConfigMap.KafkaConnections
      val zkConnections: String = clusterConfigMap.ZooKeeperConnectionString

      val jsonParm: String =
        """[{ "component" : "zookeeper", "hostslist" : "%s" }, { "component" : "kafka", "hostslist" : "%s" }, %s, { "component" : "scala", "hostslist" : "localhost" }, { "component" : "java", "hostslist" : "localhost" }]""".stripMargin.format(zkConnections, kafkaConnections, hbaseConnections)


      /**
        * Obtain the component information found on the cluster nodes.  There is a ComponentInfo created for each
        * type of information sought... namely one for scala version, java version, zookeeper, kafka and hbase. For the
        * entire cluster, this is an array of this.
        */
      val componentResults: Array[(Array[ComponentInfo], String)] = ips.map(ip => {
        val resultsFileAbsolutePath: String = s"/tmp/componentResultsFile$ip.txt"
        val (componentInfos, physicalRootDir): (Array[ComponentInfo], String) =
          getComponentVersions(log
            , rootDirPath
            , componentVersionScriptAbsolutePath
            , componentVersionJarAbsolutePath
            , ip
            , resultsFileAbsolutePath
            , jsonParm)

        if (physicalRootDir != null && physicalRootDir.length > 0) phyDirLast = physicalRootDir
        (componentInfos, physicalRootDir)
      })

      /** Let's look at the component results. comparing with expectations
        * case class ComponentInfo(version: String, status: String, errorMessage: String, componentName: String, invocationNode: String)
        */
      val scalaJavaScores: Array[(Boolean, Boolean)] = componentResults.map(pair => {
        val (components, physDir): (Array[ComponentInfo], String) = pair
        val optInfo: Option[ComponentInfo] = components.filter(component => component.componentName.toLowerCase == "scala").headOption
        val info = optInfo.orNull
        val scalaIsValid: Boolean = (info != null && info.version != null && info.version.startsWith(toScala))
        if (!scalaIsValid) {
          if (info != null) {
            log.emit(s"Scala for ip ${info.invocationNode} is invalid... msg=${info.errorMessage}")
          } else {
            log.emit("Incredible... no scala info")
          }
        }
        val joptInfo: Option[ComponentInfo] = components.filter(component => component.componentName.toLowerCase == "java").headOption
        val jinfo = joptInfo.orNull
        val javaIsValid: Boolean = (jinfo != null && jinfo.version != null && (jinfo.version.startsWith("1.7") || jinfo.version.startsWith("1.8")))
        if (!javaIsValid) {
          if (info != null) {
            log.emit(s"Java for ip ${info.invocationNode} is invalid...must be java 1.7 or java 1.8 msg=${info.errorMessage}")
          } else {
            log.emit("Incredible... no java info")
          }
        }
        (scalaIsValid, javaIsValid)

      })
      val scalaAndJavaAreValidAllNodes: Boolean = scalaJavaScores.filter(langScores => {
        val (scala, java): (Boolean, Boolean) = langScores
        (scala && java)
      }).size == componentResults.size

      /** Test if the zookeeper, kafka and hbase are healthy and suitable for use
        * FIXME: For this test, we assume the status is "ok" when the kafka can be used from the corresponding ip node */
      val zkKafkaHbaseHealthCheck: Array[(Boolean, Boolean, Boolean)] = componentResults.map(pair => {
        val (components, physDir): (Array[ComponentInfo], String) = pair
        val zkOptInfo: Option[ComponentInfo] = components.filter(component => component.componentName.toLowerCase == "zookeeper").headOption
        val info = zkOptInfo.orNull
        val zkIsValid: Boolean = (info != null && info.status != null && info.status.toLowerCase == "success")
        if (!zkIsValid) {
          if (info != null) {
            log.emit(s"Zookeeper for ip ${info.invocationNode} is not healthy... msg=${info.errorMessage}")
          } else {
            log.emit("Incredible... no zookeeper info")
          }
        }
        val kafkaOptInfo: Option[ComponentInfo] = components.filter(component => component.componentName.toLowerCase == "kafka").headOption
        val kinfo = kafkaOptInfo.orNull
        val kafkaIsValid: Boolean = (kinfo != null && kinfo.status != null && kinfo.status.toLowerCase == "success")
        if (!kafkaIsValid) {
          if (info != null) {
            log.emit(s"Kafka for ip ${kinfo.invocationNode} is not healthy... msg=${kinfo.errorMessage}")
          } else {
            log.emit("Incredible... no kafka info")
          }
        }
        val hbaseOptInfo: Option[ComponentInfo] = components.filter(component => component.componentName.toLowerCase == "hbase").headOption
        val hinfo = kafkaOptInfo.orNull
        val hbaseIsValid: Boolean = (hinfo != null && hinfo.status != null && hinfo.status.toLowerCase == "success")
        if (!hbaseIsValid) {
          if (info != null) {
            log.emit(s"HBase for ip ${hinfo.invocationNode} is not healthy... msg=${hinfo.errorMessage}")
          } else {
            log.emit("Incredible... no hbase info")
          }
        }

        (zkIsValid, kafkaIsValid, hbaseIsValid)
      })

      val zkKafkaHbaseHealthyForAllNodes: Boolean = zkKafkaHbaseHealthCheck.filter(healthTriple => {
        val (zk, kafka, hbase): (Boolean, Boolean, Boolean) = healthTriple
        (zk && kafka && hbase)
      }).size == componentResults.size

      /** Examine the physical directories on each cluster node. The physical directory returned for all of them
        * should be the same path, right?
        */
      val uniquePhysDirs: Set[String] = componentResults.map(pair => pair._2).toSet
      val physDirsAllHaveSamePath: Boolean = (uniquePhysDirs.size == 1)

      (scalaAndJavaAreValidAllNodes && zkKafkaHbaseHealthyForAllNodes && physDirsAllHaveSamePath)


    } catch {
      case e: Exception => {
        phyDirLast = rootDirPath // substitute the symbol link to see if we can test more ... this is temporary hack
        false
      }
      case t: Throwable => {
        phyDirLast = rootDirPath
        false
      }
    }

    (proposedClusterEnvironmentIsSuitable, phyDirLast)
  }

  /** **************************************************************************************************************/


  /**
    * If the supplied file path exists, answer the file name
    *
    * @param log               the installation log... if the file doesn't exist, a message about that is logged here
    * @param flPath
    * @param checkForFile      when true, the filename (last node in path) must exist in file system
    * @param checkForParentDir when true, the parent directory that prefixes the file name must exist in file system
    * @return base Filename
    */
  private def getFileName(log: InstallDriverLog, flPath: String, checkForFile: Boolean, checkForParentDir: Boolean): String = {
    val fl = new File(flPath)

    if (checkForFile && !fl.exists) {
      val msg = s"$flPath does not exists"
      log.emit(msg)
      throw new Exception(msg)
    }

    if (checkForParentDir) {
      val parentPath = fl.getParent()
      val parent = new File(parentPath)
      if (!parent.exists) {
        val msg = s"$parentPath does not exists"
        log.emit(msg)
        throw new Exception(msg)
      }
    }

    fl.getName
  }


  /**
    * Does the file path exist?
    *
    * @param flPath input file Path
    * @return whether file exists or no
    */
  private def isFileExists(flPath: String): Boolean = {
    val fl = new File(flPath)
    return fl.exists
  }

  /** ComponentInfo packages essential information about some GetComponent request's results */
  case class ComponentInfo(version: String, status: String, errorMessage: String, componentName: String, invocationNode: String)

  var _cntr = 0

  /**
    *
    * @param log                             the install driver log containing key events and messages about the installation
    * @param rootDirPath                     To get the absolute DIR name, if it is soft link otherwise it returns as it is if it exists, otherwise null
    * @param scriptAbsolutePath              Path of the script to get all the Components information
    * @param componentVersionJarAbsolutePath FAT JAR to copy to client node and execute that script
    * @param remoteNodeIp                    Remote Node IP to do scp & ssh
    * @param resultsFileAbsolutePath         Results file will be writen here and parsed to Array[ComponentInfo]
    * @param jsonArgInput                    Requested components information. Ex: [{ "component" : "zookeeper", "hostslist" : "192.168.10.20:2181,192.168.10.21:2181" } , { "component" : "kafka", "hostslist" : "localhost:9092" }, { "component" : "hbase", "hostslist" : "localhost" }, { "component" : "scala", "hostslist" : "localhost" }, { "component" : "java", "hostslist" : "localhost" }]
    * @return (Array[ComponentInfo], String) => All Components Versions and physicalRootDir
    */

  def getComponentVersions(log: InstallDriverLog
                           , rootDirPath: String
                           , scriptAbsolutePath: String
                           , componentVersionJarAbsolutePath: String
                           , remoteNodeIp: String
                           , resultsFileAbsolutePath: String
                           , jsonArgInput: String): (Array[ComponentInfo], String) = {
    val checkForJar: Boolean = true
    val checkForJarParentDir: Boolean = false
    val componentVersionJarFileName = getFileName(log, componentVersionJarAbsolutePath, checkForJar, checkForJarParentDir)
    val checkForFile: Boolean = false
    val checkForParentDir: Boolean = true
    val resultFileName = getFileName(log, resultsFileAbsolutePath, checkForFile, checkForParentDir)
    val jsonArg = jsonArgInput.replace("\r", " ").replace("\n", " ")

    _cntr += 1
    val pathOutputFlName = "__path_output_" + _cntr + "_" + math.abs(this.hashCode) + "_" + math.abs(resultFileName.hashCode) + "_" + math.abs(scriptAbsolutePath.hashCode) + "_" + math.abs(componentVersionJarFileName.hashCode)
    val getComponentInvokeCmd: String = s"$scriptAbsolutePath  --componentVersionJarAbsolutePath $componentVersionJarAbsolutePath --componentVersionJarFileName $componentVersionJarFileName --remoteNodeIp $remoteNodeIp --resultsFileAbsolutePath $resultsFileAbsolutePath --resultFileName $resultFileName --rootDirPath $rootDirPath --pathOutputFileName $pathOutputFlName --jsonArg \'$jsonArg\'"
    val getComponentsVersionCmd: Seq[String] = Seq("bash", "-c", getComponentInvokeCmd)
    _cntr += 1
    val logFile = "/tmp/__get_comp_ver_results_" + _cntr + "_" + math.abs(pathOutputFlName.hashCode) + "_" + math.abs(getComponentsVersionCmd.hashCode) + "_" + math.abs(scriptAbsolutePath.hashCode)

    log.emit(s"getComponentsVersion cmd used: $getComponentsVersionCmd")
    val getVerCmdRc: Int = (getComponentsVersionCmd #> new File(logFile)).!
    val getVerCmdResults = Source.fromFile(logFile).mkString
    if (getVerCmdRc != 0) {
      log.emit(s"getComponentsVersion has failed...rc = $getVerCmdRc")
      log.emit(s"Command used: $getComponentsVersionCmd")
      log.emit(s"Command report:\n$getVerCmdResults")
      throw new Exception("Failed to get Components Versions. Return code:" + getVerCmdRc)
    }

    val pathAbsPath = "/tmp/" + pathOutputFlName
    val physicalRootDir = if (isFileExists(pathAbsPath)) Source.fromFile("/tmp/" + pathOutputFlName).mkString else ""
    log.emit("Found PhysicalRootDir:" + physicalRootDir)

    var results = ArrayBuffer[ComponentInfo]()
    try {
      val jsonStr = Source.fromFile(resultsFileAbsolutePath).mkString
      log.emit("Components Results:" + jsonStr)
      implicit val jsonFormats = org.json4s.DefaultFormats
      val json = org.json4s.jackson.JsonMethods.parse(jsonStr)
      if (json == null) {
        throw new Exception("Failed to parse Components Versions json : " + jsonStr)
      }

      var components = List[Any]()

      if (json.values.isInstanceOf[List[_]])
        components = json.values.asInstanceOf[List[_]]
      else
        components = List(json.values)

      components.foreach(c => {
        val tmpmap = c.asInstanceOf[Map[String, Any]]
        val version = tmpmap.getOrElse("version", null)
        val status = tmpmap.getOrElse("status", null)
        val errorMessage = tmpmap.getOrElse("errorMessage", null)
        val componentName = tmpmap.getOrElse("componentName", null)
        val invocationNode = tmpmap.getOrElse("invocationNode", remoteNodeIp)

        results += ComponentInfo(if (version == null) "" else version.toString
          , if (status == null) "" else status.toString
          , if (errorMessage == null) "" else errorMessage.toString
          , if (componentName == null) "" else componentName.toString
          , if (invocationNode == null) "" else invocationNode.toString)
      })
    } catch {
      case e: Exception => throw new Exception("Failed to parse Components Versions json", e)
      case e: Throwable => throw new Exception("Failed to parse Components Versions json", e)
    }

    (results.toArray, physicalRootDir)
  }

  /** **************************************************************************************************************/

  def collectNodeInfo(log: InstallDriverLog, clusterId: String, clusterNodes: List[Map[String, Any]], installDir: String)
  : (Array[String], Array[(String, String, String, String)], Array[(String, String)]) = {
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

    val ipIdTargPathsSet: Set[(String, String, String, String)] = clusterNodes.map(info => {
      val nodeInfo: Map[String, _] = info.asInstanceOf[Map[String, _]]
      val ipAddr: String = nodeInfo.getOrElse("NodeIpAddr", "_bo_gu_us_node_ip_ad_dr").asInstanceOf[String]
      val nodeId: String = nodeInfo.getOrElse("NodeId", "unkn_own_node_id").asInstanceOf[String]
      val roles: List[String] = nodeInfo.getOrElse("Roles", List[String]()).asInstanceOf[List[String]]
      val rolesStr: String = if (roles.size > 0) roles.mkString(",") else "UNKNOWN_ROLES"
      (ipAddr, nodeId, configDir, rolesStr)
    }).toSet
    val ipIdTargPathsReasonable: Boolean = ipIdTargPathsSet.filter(quartet => {
      val (ipAddr, nodeId, _, rolesStr): (String, String, String, String) = quartet
      (ipAddr == "_bo_gu_us_node_ip_ad_dr" || nodeId == "unkn_own_node_id" || rolesStr == "UNKNOWN_ROLES")
    }).size == 0
    if (!ipIdTargPathsReasonable) {
      log.emit(s"the node ip addr, node identifier, and/or node roles are bad for cluster id $clusterId ... aborting")
      log.close
      sys.exit(1)
    }
    val ipIdTargPaths: Array[(String, String, String, String)] = ipIdTargPathsSet.toSeq.sorted.toArray

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
  def mapProperties(log: InstallDriverLog, apiConfigPath: String): Properties = {

    val (properties, errorDescr): (Properties, String) = Utils.loadConfiguration(apiConfigPath, true)

    if (errorDescr != null) {
      log.emit(s"The apiConfigPath properties path ($apiConfigPath) could not produce a valid set of properties...aborting")
      log.close
      sys.exit(1)
    }

    properties
  }

  /**

    * @param log
    * @param physicalRootDir
    * @param fromKamanjaVer
    * @param toKamanjaVer
    * @return
    */

  def CreateInstallationNames(log: InstallDriverLog, physicalRootDir: String, fromKamanjaVer: String, toKamanjaVer: String)
  : (String, String, String) = {


    val parentPath: String = physicalRootDir.split('/').dropRight(1).mkString("/")
    val phyDirName: String = physicalRootDir.split('/').last
    val dateTime: DateTime = new DateTime
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd_HHmmss")
    val datestr: String = fmt.print(dateTime);
    /*
    // ROOT_DIR/parPath as /apps/KamanjaInstall can be link or dir. If it has link that is prior installation. Otherwise create as below

    /apps/KamanjaInstall =>

    /apps/KamanjaInstall_pre_<datetime> if  ROOT_DIR/parPath is DIR

    /apps/KamanjaInstall_<version>_<datetime>
    */
    val priorInstallDirName: String = s"${phyDirName}_${fromKamanjaVer}_${datestr}"
    val newInstallDirName: String = s"${phyDirName}_${toKamanjaVer}_${datestr}"

    (parentPath, priorInstallDirName, newInstallDirName)
  }

  private def CheckInstallVerificationFile(log: InstallDriverLog, fl: String, ipPathPairs: Array[(String, String)], newInstallDirName: String, physicalRootDir: String): Boolean = {
    val allValues = ArrayBuffer[Array[String]]()
    val allLines = ArrayBuffer[String]()
    logger.info(fl + " contents")
    for (line <- Source.fromFile(fl).getLines()) {
      logger.info(line)
      val vals = line.split(",")
      if (vals.size != 7) {
        val errMsg = "Expecting HostName,LinkDir,LinkExists(Yes|No),LinkPointingToDir,LinkPointingDirExists(Yes|No),NewInstallDir, NewInstallDirExists(Yes|No). But found:" + line
        log.emit(errMsg)
        logger.error(errMsg)
      } else {
        allValues += vals;
        allLines += line
      }
    }

    // Checke whether we have all nodes or not
    if (ipPathPairs.size != allValues.size) {
      val errMsg = "Suppose to get verification information for %d nodes. But we got only for %d".format(ipPathPairs.size, allValues.size)
      log.emit(errMsg)
      logger.error(errMsg)
      return false
    }

    var isInvalid = false
    // Starting with index 0
    // #3 & #5 should match
    // #2, #4, #6 should have only Yes
    // #5 & newInstallDirName should match
    allValues.foreach(av => {
      if (av(3).compare(av(5)) != 0) {
        val errMsg = ("LinkPointingToDir:%s != NewInstallDir:%s from %s".format(av(3), av(5), av.mkString(",")))
        log.emit(errMsg)
        logger.error(errMsg)
        isInvalid = true;
      }
      if (av(2).equalsIgnoreCase("No") || av(4).equalsIgnoreCase("No") || av(6).equalsIgnoreCase("No")) {
        val errMsg = ("LinkExists/LinkPointingDirExists/NewInstallDirExists is NO from %s".format(av.mkString(",")))
        log.emit(errMsg)
        logger.error(errMsg)
        isInvalid = true;
      }
      if (av(5).compare(newInstallDirName) != 0) {
        val errMsg = ("NewInstallDir:%s != newInstallDirName:%s from %s".format(av(3), newInstallDirName, av.mkString(",")))
        log.emit(errMsg)
        logger.error(errMsg)
        isInvalid = true;
      }
    })

    if (isInvalid)
      return false;

    val map = allValues.map(av => ((av(0).toLowerCase() + "," + av(1)), av)).toMap

    val notFoundNodes = ArrayBuffer[String]()

    ipPathPairs.foreach(ipAndPath => {
      val foundNode = map.getOrElse((ipAndPath._1.toLowerCase() + "," + ipAndPath._2), null)

      if (foundNode == null) {
        isInvalid = true
        notFoundNodes += "(Host:%s & TargetPath:%s)".format(ipAndPath._1.toLowerCase(), ipAndPath._2)
      }
    })

    if (isInvalid) {
      val errMsg = ("%s not found in %s ".format(notFoundNodes.mkString(","), allLines.mkString(",")))
      log.emit(errMsg)
      logger.error(errMsg)
      return false;
    }

    return true
  }


  /**
    * Call the KamanjaClusterInstall with the following parameters that it needs to install the software on the cluster.
    * Important events encountered during the installation are added to the InstallDriverLog instance.
    * Semantics:
    *
    * The installation is performed and as part of the result a file is produced that has information in it like this that
    * is used to verify the installation. There will be a row for each cluster node.  The fields are compared/contrasted
    * to see if the installation was successful.  Here is sample file content:
    *
    * # {HostName,LinkDir,LinkExists(Yes|No),LinkPointingToDir,LinkPointingDirExists(Yes|No),NewInstallDir, NewInstallDirExists(Yes|No)}

    * 10.100.0.53,/opt/KamanjaCluster-2.11,Yes,/opt/KamanjaCluster-2.11_20160218231108,Yes,/opt/KamanjaCluster-2.11_20160218231108,Yes
    * 10.100.0.54,/opt/KamanjaCluster-2.11,Yes,/opt/KamanjaCluster-2.11_20160218231108,Yes,/opt/KamanjaCluster-2.11_20160218231108,Yes
    * 10.100.0.55,/opt/KamanjaCluster-2.11,Yes,/opt/KamanjaCluster-2.11_20160218231108,Yes,/opt/KamanjaCluster-2.11_20160218231108,Yes

    * Checks: 1) All should be Yes/No should be Yes
    * 2) LinkPointingToDir and NewInstallDir should be equivalent
    * 3) The newInstallDirName passed should match  NewInstallDir
    * 4) LinkDir and API config property ROOT_DIR value should match
    *
    * CheckInstallVerificationFile check for this information
    *

    * @param log                 the InstallDriverLog that tracks progress and important events of this installation
    * @param clusterInstallerDriversLocation the location of the clusterInstallerDriver AND the KamanjaClusterInstall.sh called here
    * @param physicalRootDir     the actual root dir for the installation
    * @param apiConfigPath       the api config that contains seminal information about the cluster installation
    * @param nodeConfigPath      the node config that contains the cluster description used for the installation
    * @param priorInstallDirName the name of the directory to be used for a prior installation that is being upgraded (if appropriate)
    * @param newInstallDirName   the new installation directory name that will live in parentPath
    * @param tarballPath         the local tarball path that contains the kamanja installation
    * @param ips                 a file path that contains the unique ip addresses for each node in the cluster
    * @param ipIdTargPaths       a file path that contains the ip address, node id, target dir path and roles
    * @param ipPathPairs         a file containing the unique ip addresses and path pairs
    * @return true if the installation succeeded.
    */
  def installCluster(log: InstallDriverLog
                     , clusterInstallerDriversLocation: String
                     , physicalRootDir: String
                     , apiConfigPath: String
                     , nodeConfigPath: String
                     , priorInstallDirName: String
                     , newInstallDirName: String
                     , tarballPath: String
                     , ips: Array[String]
                     , ipIdTargPaths: Array[(String, String, String, String)]
                     , ipPathPairs: Array[(String, String)]): Boolean = {

    // Check for KamanjaClusterInstall.sh existance. And see whether KamanjaClusterInstall.sh has all error handling or not.
    val parentPath: String = physicalRootDir.split('/').dropRight(1).mkString("/")
    val scriptExitsCheck: Seq[String] = Seq("bash", "-c", s"$clusterInstallerDriversLocation/KamanjaClusterInstall.sh")
    val scriptExistsCmdRc = Process(scriptExitsCheck).!
    if (scriptExistsCmdRc != 0) {
      log.emit(s"KamanjaClusterInstall script is not installed... it must be on your PATH... rc = $scriptExistsCmdRc")
      log.emit("Installation is aborted. Consult the log file (${log.logPath}) for details.")
      log.close
      sys.exit(1)
    }

    val dateTime: DateTime = new DateTime
    val fmt: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd_HHmmss")
    val datestr: String = fmt.print(dateTime);
    val verifyFilePath: String = "/tmp/__KamanjaClusterResults_$datestr"

    val priorInstallDirPath: String = s"$parentPath/$priorInstallDirName"
    val newInstallDirPath: String = s"$parentPath/$newInstallDirName"
    val installCmd: Seq[String] = Seq("bash", "-c", s"$clusterInstallerDriversLocation/KamanjaClusterInstall.sh  --MetadataAPIConfig $apiConfigPath --NodeConfigPath $nodeConfigPath --ParentPath $parentPath --PriorInstallDirName $priorInstallDirName --NewInstallDirName $newInstallDirName --TarballPath $tarballPath --ipAddrs $ips --ipIdTargPaths $ipIdTargPaths --ipPathPairs $ipPathPairs --priorInstallDirPath $priorInstallDirPath --newInstallDirPath $newInstallDirPath --installVerificationFile $verifyFilePath ")
    val installCmdRep: String = installCmd.mkString(" ")
    log.emit(s"KamanjaClusterInstall cmd used: \n\n$installCmdRep")

    //println(s"KamanjaClusterInstall cmd used: \n\n$installCmdRep\n")

    val installCmdRc: Int = (installCmd #> new File("/tmp/__install_results_")).!
    val installCmdResults: String = Source.fromFile("/tmp/__install_results_").mkString
    if (installCmdRc != 0) {
      log.emit(s"KamanjaClusterInstall has failed...rc = $installCmdRc")
      log.emit(s"Command used: $installCmd")
      log.emit(s"Command report:\n$installCmdResults")
      log.emit(s"Installation is aborted. Consult the log file (${log.logPath}) for details.")
      log.close
      sys.exit(1)
    } else {
      if (!CheckInstallVerificationFile(log, verifyFilePath, ipPathPairs, newInstallDirName, physicalRootDir)) {
        log.emit("Failed to verify information collected from installation.")
        log.close
        sys.exit(1)
      }
    }
    log.emit(s"New installation command result report:\n$installCmdResults")

    (installCmdRc == 0)
  }

  /**
    * runFromJsonConfigString(jsonConfigString : String) : Int
    *
    * @param log                      InstallDriverLog instance that records installation process events
    * @param apiConfigFile            the local path location of the metadata api configuration file
    * @param apiConfigMap             a Properties file version instance of that fil's content
    * @param nodeConfigPath           the cluster nodes configuration file
    * @param migrateConfigFilePath    the path to the migrateTemplate
    * @param unhandledMetadataDumpDir the location where logs and unhandledMetadata is dumped
    * @param fromKamanja              which Kamanja is being upgraded (e.g., 1.1 or 1.2)
    * @param fromScala                the Scala for the existing implementation (2.10)
    * @param toScala                  the Scala for the new installation (either 2.10 or 2.11)
    * @param parentPath               the directory that will contain the new and prior installation
    * @param priorInstallDirName      the physical name of the prior installation (after installation completes)
    * @param newInstallDirName        the physical name of the new installation
    * @return
    */
  def doMigration(log: InstallDriverLog
                  , apiConfigFile: String
                  , apiConfigMap: Properties
                  , nodeConfigPath: String
                  , migrateConfigFilePath: String
                  , unhandledMetadataDumpDir: String
                  , fromKamanja: String
                  , fromScala: String
                  , toScala: String
                  , parentPath: String
                  , priorInstallDirName: String
                  , newInstallDirName: String): Boolean = {

    val migrationToBeDone: String = if (fromKamanja == "1.1") "1.1=>1.3" else if (fromKamanja == "1.2") "1.2=>1.3" else "hmmm"

    val upgradeOk: Boolean = migrationToBeDone match {
      case "1.1=>1.3" => {
        val kamanjaFromVersion: String = "1.1"
        val kamanjaFromVersionWithUnderscore: String = "1_1"
        val migrateConfigJSON: String = createMigrationConfig(log
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
        val migrateObj: Migrate = new Migrate()
        migrateObj.registerStatusCallback(log)
        val rc: Int = migrateObj.runFromJsonConfigString(migrateConfigJSON)
        (rc == 0)
      }
      case "1.2=>1.3" => {
        val kamanjaFromVersion: String = "1.1"
        val kamanjaFromVersionWithUnderscore: String = "1_1"
        val migrateConfigJSON: String = createMigrationConfig(log
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
        val migrateObj: Migrate = new Migrate()
        migrateObj.registerStatusCallback(log)
        val rc: Int = migrateObj.runFromJsonConfigString(migrateConfigJSON)
        (rc == 0)
      }
      case _ => {
        log.emit("The 'fromKamanja' parameter is incorrect... this needs to be fixed.  The value can only be '1.1' or '1.2' for the '1.3' upgrade")
        false
      }
    }
    if (!upgradeOk) {
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
  @param filePath      the path of the file that will be created.  It should be valid... no checks here.

    */
  private def writeCfgFile(cfgString: String, filePath: String) {
    val file = new File(filePath);
    val bufferedWriter = new BufferedWriter(new FileWriter(file))
    bufferedWriter.write(cfgString)
    bufferedWriter.close
  }


  /**
    * Answer a migration configuration json structure that contains the requisite migration info required for one of the migration/upgrade paths.
    *
    * @param log
    * @param migrateConfigFilePath            the local file system path where the template file is found
    * @param clusterConfigFile                substitution value
    * @param apiConfigFile                    substitution value
    * @param kamanjaFromVersion               substitution value
    * @param kamanjaFromVersionWithUnderscore substitution value
    * @param newPackageInstallPath            substitution value
    * @param oldPackageInstallPath            substitution value
    * @param scalaFromVersion                 substitution value
    * @param scalaToVersion                   substitution value
    * @param unhandledMetadataDumpDir         substitution value
    * @return filled in template with supplied values.
    */
  def createMigrationConfig(log: InstallDriverLog
                            , migrateConfigFilePath: String
                            , clusterConfigFile: String
                            , apiConfigFile: String
                            , kamanjaFromVersion: String
                            , kamanjaFromVersionWithUnderscore: String
                            , newPackageInstallPath: String
                            , oldPackageInstallPath: String
                            , scalaFromVersion: String
                            , scalaToVersion: String
                            , unhandledMetadataDumpDir: String
                           ): String = {

    val template: String = Source.fromFile(migrateConfigFilePath).mkString

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

    val subPairs = Map[String, String]("ClusterConfigFile" -> clusterConfigFile
      , "ApiConfigFile" -> apiConfigFile
      , "KamanjaFromVersion" -> kamanjaFromVersion
      , "KamanjaFromVersionWithUnderscore" -> kamanjaFromVersionWithUnderscore
      , "NewPackageInstallPath" -> newPackageInstallPath
      , "OldPackageInstallPath" -> oldPackageInstallPath
      , "ScalaFromVersion" -> scalaFromVersion
      , "ScalaToVersion" -> scalaToVersion
      , "UnhandledMetadataDumpDir" -> unhandledMetadataDumpDir)

    val substitutionMap: Map[String, String] = subPairs.toMap
    val varSub = new MapSubstitution(template, substitutionMap)
    val substitutedTemplate: String = varSub.makeSubstitutions
    substitutedTemplate
  }


}

class ClusterConfigMap(cfgStr: String, clusterIdOfInterest: String) {

  val clusterMap: Map[String, Any] = getClusterConfigMapOfInterest(cfgStr, clusterIdOfInterest)
  if (clusterMap.size == 0) {
    throw new RuntimeException(s"There is no cluster information for cluster $clusterIdOfInterest")
  }
  val clusterId: String = getClusterId
  val dataStore: Map[String, Any] = getDataStore
  val zooKeeperInfo: Map[String, Any] = getZooKeeperInfo
  val environmentContext: Map[String, Any] = getEnvironmentContext
  val clusterNodes: List[Map[String, Any]] = getClusterNodes
  val adapters: List[Map[String, Any]] = getAdapters

  def ClusterMap: Map[String, Any] = clusterMap

  def ClusterId: String = clusterId

  def DataStore: Map[String, Any] = dataStore

  def ZooKeeperInfo: Map[String, Any] = zooKeeperInfo

  def EnvironmentContext: Map[String, Any] = environmentContext

  def ClusterNodes: List[Map[String, Any]] = clusterNodes

  def Adapters: List[Map[String, Any]] = adapters

  def ZooKeeperConnectionString: String = {
    zooKeeperInfo.getOrElse("ZooKeeperConnectString", "").asInstanceOf[String]
  }

  def KafkaConnections: String = {
    val kafkaAdapters: List[Map[String, Any]] = adapters.filter(adapterMap => {
      val adapterJars: List[String] = adapterMap.getOrElse("DependencyJars", "").asInstanceOf[List[String]]
      val hasKafka: Boolean = adapterJars.filter(jarName => jarName.contains("kafka_2")).nonEmpty
      hasKafka
    })
    val hostConnections: List[String] = kafkaAdapters.map(adapter => {
      val adapterSpecificCfg: Map[String, Any] = adapter.getOrElse("AdapterSpecificCfg", Map[String, Any]()).asInstanceOf[Map[String, Any]]
      val conn: String = adapterSpecificCfg.getOrElse("HostList", "").asInstanceOf[String]
      conn
    })
    hostConnections.toSet.mkString(",")
  }

  /**
    * getStringFromJsonNode
    *
    * @param v just any old thing
    * @return a string representation
    */
  private def getStringFromJsonNode(v: Map[String, Any]): String = {
    if (v == null) return ""
    implicit val jsonFormats: Formats = DefaultFormats
    return Serialization.write(v)
  }

  def DataStoreConnections: String = {

    /** FIXME: does this require something more to format as json string? */
    getStringFromJsonNode(DataStore)
  }


  private def getClusterConfigMapOfInterest(cfgStr: String, clusterIdOfInterest: String): Map[String, Any] = {
    val clusterMap: Map[String, Any] = try {
      // extract config objects
      val map = JsonSerializer.parseEngineConfig(cfgStr)
      // process clusterInfo object if it exists
      val mapOfInterest: Map[String, Any] = if (map.contains("Clusters")) {
        val clusterList: List[_] = map.get("Clusters").get.asInstanceOf[List[_]]
        val clusters = clusterList.length

        val clusterSought: String = clusterIdOfInterest.toLowerCase
        val clusterIdList: List[Any] = clusterList.filter(aCluster => {
          val cluster: Map[String, Any] = aCluster.asInstanceOf[Map[String, Any]]
          val clusterId = cluster.getOrElse("ClusterId", "").toString.trim.toLowerCase
          (clusterId == clusterSought)
        })

        val clusterOfInterestMap: Map[String, Any] = if (clusterIdList.size > 0) {
          clusterIdList.head.asInstanceOf[Map[String, Any]]
        } else {
          Map[String, Any]()
        }
        clusterOfInterestMap
      } else {
        Map[String, Any]()
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
    val clustId: String = if (clusterMap.size > 0 && clusterMap.contains("ClusterId")) {
      clusterMap("ClusterId").asInstanceOf[String]
    } else {
      ""
    }
    clustId
  }

  private def getDataStore: Map[String, Any] = {
    val dataStoreMap: Map[String, Any] = if (clusterMap.size > 0 && clusterMap.contains("DataStore")) {
      clusterMap("DataStore").asInstanceOf[Map[String, Any]]
    } else {
      Map[String, Any]()
    }
    dataStoreMap
  }

  private def getZooKeeperInfo: Map[String, Any] = {
    val dataStoreMap: Map[String, Any] = if (clusterMap.size > 0 && clusterMap.contains("ZooKeeperInfo")) {
      clusterMap("ZooKeeperInfo").asInstanceOf[Map[String, Any]]
    } else {
      Map[String, Any]()
    }
    dataStoreMap
  }

  private def getEnvironmentContext: Map[String, Any] = {
    val envCtxMap: Map[String, Any] = if (clusterMap.size > 0 && clusterMap.contains("EnvironmentContext")) {
      clusterMap("EnvironmentContext").asInstanceOf[Map[String, Any]]
    } else {
      Map[String, Any]()
    }
    envCtxMap
  }

  private def getClusterNodes: List[Map[String, Any]] = {
    val nodeMapList: List[Map[String, Any]] = if (clusterMap.size > 0 && clusterMap.contains("Nodes")) {
      clusterMap("Nodes").asInstanceOf[List[Map[String, Any]]]
    } else {
      List[Map[String, Any]]()
    }
    nodeMapList
  }

  private def getAdapters: List[Map[String, Any]] = {
    val adapterMapList: List[Map[String, Any]] = if (clusterMap.size > 0 && clusterMap.contains("Adapters")) {
      clusterMap("Adapters").asInstanceOf[List[Map[String, Any]]]
    } else {
      List[Map[String, Any]]()
    }
    adapterMapList
  }

}

class MapSubstitution(template: String, vars: scala.collection.immutable.Map[String, String]) {

  def findAndReplace(m: Matcher)(callback: String => String): String = {
    val sb = new StringBuffer
    while (m.find) {
      val replStr = vars(m.group(1))
      m.appendReplacement(sb, callback(replStr))
    }
    m.appendTail(sb)
    sb.toString
  }

  def makeSubstitutions: String = {
    val m = Pattern.compile("""({[A-Za-z0-9_.-]+})""").matcher(template)
    findAndReplace(m) { x => x }
  }

}

