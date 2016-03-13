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
  var locations : Array[String] = Array.empty[String] //folders to monitor

  var connectionConfig : FileAdapterConnectionConfig = null
  var monitoringConfig : FileAdapterMonitoringConfig = null
}

class FileAdapterConnectionConfig {
  var hostsList: Array[String] = Array.empty[String]
  var userId: String = _
  var password: String = _
}
class FileAdapterMonitoringConfig {
  var waitingTimeMS : Int = _
}

object SmartFileAdapterConfiguration{

  val defaultWaitingTimeMS = 1000

  def GetAdapterConfig(inputConfig: AdapterConfiguration): SmartFileAdapterConfiguration = {

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

    val adapCfg = parse(inputConfig.adapterSpecificCfg)
    if (adapCfg == null || adapCfg.values == null) {
      val err = "Not found Type and Connection info for Smart File Adapter Config:" + inputConfig.Name
      throw new KamanjaException(err, null)
    }

    val adapCfgValues = adapCfg.values.asInstanceOf[Map[String, Any]]

    if(adapCfgValues.getOrElse("Type", null) == null) {
      val err = "Not found Type for Smart File Adapter Config:" + inputConfig.Name
      throw new KamanjaException(err, null)
    }
    adapterConfig._type = adapCfgValues.get("Type").get.toString

    if(adapCfgValues.getOrElse("Locations", null) == null) {
      val err = "Not found Locations for Smart File Adapter Config:" + inputConfig.Name
      throw new KamanjaException(err, null)
    }
    adapterConfig.locations = adapCfgValues.get("Locations").get.toString.split(",").map(str => str.trim).filter(str => str.size > 0)

    adapterConfig.connectionConfig = new FileAdapterConnectionConfig()
    adapterConfig.monitoringConfig = new FileAdapterMonitoringConfig()

    if(adapCfgValues.getOrElse("ConnectionConfig", null) == null){
      val err = "Not found ConnectionConfig for Smart File Adapter Config:" + inputConfig.Name
      throw new KamanjaException(err, null)
    }

    val connConf = adapCfgValues.get("ConnectionConfig").get.asInstanceOf[Map[String, String]]
    //val connConfValues = connConf.values.asInstanceOf[Map[String, String]]
    connConf.foreach(kv => {
      if (kv._1.compareToIgnoreCase("HostLists") == 0) {
        adapterConfig.connectionConfig.hostsList = kv._2.split(",").map(str => str.trim).filter(str => str.size > 0)
      } else if (kv._1.compareToIgnoreCase("UserId") == 0) {
        adapterConfig.connectionConfig.userId = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("Password") == 0) {
        adapterConfig.connectionConfig.password = kv._2.trim
      }
    })

    if(adapCfgValues.getOrElse("MonitoringConfig", null) == null){
      val err = "Not found MonitoringConfig for Smart File Adapter Config:" + inputConfig.Name
      throw new KamanjaException(err, null)
    }
    val monConf = (adapCfgValues.get("MonitoringConfig").get.asInstanceOf[Map[String, String]])
    //val monConfValues = monConf.values.asInstanceOf[Map[String, String]]
    monConf.foreach(kv => {
      if (kv._1.compareToIgnoreCase("MaxTimeWait") == 0) {
        adapterConfig.monitoringConfig.waitingTimeMS = kv._2.trim.toInt
        if (adapterConfig.monitoringConfig.waitingTimeMS < 0)
          adapterConfig.monitoringConfig.waitingTimeMS = defaultWaitingTimeMS
      }
    })

    adapterConfig
  }
}