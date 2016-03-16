package com.ligadata.BasicCacheConcurrency


import net.sf.ehcache.{Element, Cache, CacheManager}
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConverters._

/**
  * Created by Saleh on 3/15/2016.
  */
class MemoryDataCacheImp extends DataCache{

  var cm:CacheManager = null
  var cache:Cache = null
  var xmlPath:String = null
  var cacheName:String = null

  override def init(jsonString:String): Unit = {

    val json = parse(jsonString)
    val values = json.values.asInstanceOf[Map[String, String]]
    xmlPath = values.getOrElse("xmlPath", null)
    cacheName = values.getOrElse("cacheName", null)

  }

  override def putInCache(map: java.util.Map[_, _]): Unit = {
    val scalaMap = map.asScala
    scalaMap.foreach {keyVal => cache.put(new Element(keyVal._1,keyVal._2))}
  }

  override def getFromCache(key: String): Object = {
    val ele:Element = cache.get(key)

    ele.getObjectValue
  }

  override def start(): Unit = {
    cm = CacheManager.newInstance(xmlPath)
    cache = cm.getCache(cacheName)
  }

  override def shutdwon(): Unit = {
    cm.shutdown()
  }

}
