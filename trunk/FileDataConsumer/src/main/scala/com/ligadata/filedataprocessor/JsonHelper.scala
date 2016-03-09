package com.ligadata.filedataprocessor

import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.Formats

import com.ligadata.Exceptions.KamanjaException

/**
  * Created by Yasser on 3/9/2016.
  */

object JsonHelper {
  def getConnectionConfigObj(jsonStr : String): ConnectionConfig = {

    val map = parse(jsonStr).values.asInstanceOf[Map[String, Any]]
    if(map.getOrElse("Type", null) == null)
      throw new KamanjaException("Monitoring Connection Type must be provided", null)

    if(map.getOrElse("Host", null) == null)
      throw new KamanjaException("Monitoring Connection Host must be provided", null)

    if(map.getOrElse("Locations", null) == null)
      throw new KamanjaException("Monitoring Connection Locations must be provided", null)
    val locations =
      try {
        map.get("Locations").get.asInstanceOf[List[String]].toArray
      }
      catch {
        case e : Exception => throw new KamanjaException("Monitoring Connection Locations is invalid", null)
      }

     new ConnectionConfig(map.get("Type").get.toString, map.get("Host").get.toString, locations, map.getOrElse("UserId", null).toString, map.getOrElse("Password", null).toString)
  }

  def getMonitoringConfigObj(jsonStr : String): MonitoringConfig = {

    val map = parse(jsonStr).values.asInstanceOf[Map[String, Any]]

    if(map.getOrElse("WaitingTimeMS", null) == null)
      throw new KamanjaException("Monitoring WaitingTimeMS must be provided", null)

    val waitingTimeMSStr =  map.get("WaitingTimeMS").get.toString
    val waitingTimeMS =
      try {
         waitingTimeMSStr.toInt
      }
      catch{
        case e : Exception => throw new KamanjaException("Monitoring WaitingTimeMS is invalid", null)
      }

    new MonitoringConfig(waitingTimeMS)
  }
}
