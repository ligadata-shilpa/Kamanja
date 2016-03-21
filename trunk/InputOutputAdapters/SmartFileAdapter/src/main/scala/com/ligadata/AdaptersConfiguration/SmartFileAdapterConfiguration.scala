package com.ligadata.AdaptersConfiguration

import com.ligadata.Exceptions.KamanjaException
import com.ligadata.InputOutputAdapterInfo.AdapterConfiguration
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._

/**
  * Created by Yasser on 3/10/2016.
  */
class SmartFileAdapterConfiguration extends AdapterConfiguration {
  var _type: String = _ // FileSystem, hdfs, sftp

  var connectionConfig : FileAdapterConnectionConfig = null
  var monitoringConfig : FileAdapterMonitoringConfig = null
}

class FileAdapterConnectionConfig {
  var hostsList: Array[String] = Array.empty[String]
  var userId: String = _
  var password: String = _
  var authentication: String = _
  var principal: String = _
  var keytab: String = _
}

class FileAdapterMonitoringConfig {
  var waitingTimeMS : Int = _
  var locations : Array[String] = Array.empty[String] //folders to monitor

  var fileBufferingTimeout = 300 // in seconds
  var metadataConfigFile = ""
  var targetMoveDir = ""
}

object SmartFileAdapterConfiguration{

  val defaultWaitingTimeMS = 1000

  def getAdapterConfig(inputConfig: AdapterConfiguration): SmartFileAdapterConfiguration = {

    if (inputConfig.adapterSpecificCfg == null || inputConfig.adapterSpecificCfg.size == 0) {
      val err = "Not found Type and Connection info for Smart File Adapter Config:" + inputConfig.Name
      throw new KamanjaException(err, null)
    }

    val adapterConfig = new SmartFileAdapterConfiguration()
    adapterConfig.Name = inputConfig.Name
    adapterConfig.formatName = inputConfig.formatName
    adapterConfig.validateAdapterName = inputConfig.validateAdapterName
    adapterConfig.failedEventsAdapterName = inputConfig.failedEventsAdapterName
    adapterConfig.className = inputConfig.className
    adapterConfig.jarName = inputConfig.jarName
    adapterConfig.dependencyJars = inputConfig.dependencyJars
    adapterConfig.associatedMsg = if (inputConfig.associatedMsg == null) null else inputConfig.associatedMsg.trim
    adapterConfig.keyAndValueDelimiter = if (inputConfig.keyAndValueDelimiter == null) null else inputConfig.keyAndValueDelimiter.trim
    adapterConfig.fieldDelimiter = if (inputConfig.fieldDelimiter == null) null else inputConfig.fieldDelimiter.trim
    adapterConfig.valueDelimiter = if (inputConfig.valueDelimiter == null) null else inputConfig.valueDelimiter.trim

    val (_type, connectionConfig, monitoringConfig) = parseSmartFileAdapterSpecificConfig(inputConfig.Name, inputConfig.adapterSpecificCfg)
    adapterConfig._type = _type
    adapterConfig.connectionConfig = connectionConfig
    adapterConfig.monitoringConfig = monitoringConfig

    adapterConfig
  }

  def parseSmartFileAdapterSpecificConfig(adapterName : String, adapterSpecificCfgJson : String) : (String, FileAdapterConnectionConfig, FileAdapterMonitoringConfig) = {

    val adapCfg = parse(adapterSpecificCfgJson)

    if (adapCfg == null || adapCfg.values == null) {
      val err = "Not found Type and Connection info for Smart File Adapter Config:" + adapterName
      throw new KamanjaException(err, null)
    }

    val adapCfgValues = adapCfg.values.asInstanceOf[Map[String, Any]]

    if(adapCfgValues.getOrElse("Type", null) == null) {
      val err = "Not found Type for Smart File Adapter Config:" + adapterName
      throw new KamanjaException(err, null)
    }
    val _type = adapCfgValues.get("Type").get.toString

    val connectionConfig = new FileAdapterConnectionConfig()
    val monitoringConfig = new FileAdapterMonitoringConfig()

    if(adapCfgValues.getOrElse("ConnectionConfig", null) == null){
      val err = "Not found ConnectionConfig for Smart File Adapter Config:" + adapterName
      throw new KamanjaException(err, null)
    }

    val connConf = adapCfgValues.get("ConnectionConfig").get.asInstanceOf[Map[String, String]]
    //val connConfValues = connConf.values.asInstanceOf[Map[String, String]]
    connConf.foreach(kv => {
      if (kv._1.compareToIgnoreCase("HostLists") == 0) {
        connectionConfig.hostsList = kv._2.split(",").map(str => str.trim).filter(str => str.size > 0)
      } else if (kv._1.compareToIgnoreCase("UserId") == 0) {
        connectionConfig.userId = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("Password") == 0) {
        connectionConfig.password = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("Authentication") == 0) {
        connectionConfig.authentication = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("Principal") == 0) {
        connectionConfig.principal = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("Keytab") == 0) {
        connectionConfig.keytab = kv._2.trim
      }
    })
    if(connectionConfig.authentication == null || connectionConfig.authentication == "")
      connectionConfig.authentication = "simple"//default

    if(adapCfgValues.getOrElse("MonitoringConfig", null) == null){
      val err = "Not found MonitoringConfig for Smart File Adapter Config:" + adapterName
      throw new KamanjaException(err, null)
    }
    val monConf = (adapCfgValues.get("MonitoringConfig").get.asInstanceOf[Map[String, String]])
    //val monConfValues = monConf.values.asInstanceOf[Map[String, String]]
    monConf.foreach(kv => {
      if (kv._1.compareToIgnoreCase("MaxTimeWait") == 0) {
        monitoringConfig.waitingTimeMS = kv._2.trim.toInt
        if (monitoringConfig.waitingTimeMS < 0)
          monitoringConfig.waitingTimeMS = defaultWaitingTimeMS
      } else  if (kv._1.compareToIgnoreCase("Locations") == 0) {
        monitoringConfig.locations = kv._2.split(",").map(str => str.trim).filter(str => str.size > 0)
      }
    })

    if(monitoringConfig.locations == null || monitoringConfig.locations.length == 0) {
      val err = "Not found Locations for Smart File Adapter Config:" + adapterName
      throw new KamanjaException(err, null)
    }

    (_type, connectionConfig, monitoringConfig)
  }
}