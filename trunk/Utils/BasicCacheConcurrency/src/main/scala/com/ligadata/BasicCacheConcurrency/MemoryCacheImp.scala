package com.ligadata.BasicCacheConcurrency

import java.util._


import net.sf.ehcache.{Element, Cache, CacheManager}

import scala.collection.JavaConverters._

/**
  * Created by Saleh on 3/15/2016.
  */
class MemoryCacheImp extends MemoryCache{

  var cm:CacheManager = null
  var cache:Cache = null

  override def init(): Unit = ???

  override def putInCache(map: Map[_, _]): Unit = {
    val scalaMap = map.asScala
    scalaMap.foreach {keyVal => cache.put(new Element(keyVal._1,keyVal._2))}
  }

  override def getFromCache(key: String): Object = {
    val ele:Element = cache.get(key)

    ele.getObjectValue
  }

  override def start(): Unit = {
    cm = CacheManager.newInstance()
    cache = cm.getCache("userCache")
  }

  override def shutdwon(): Unit = {
    cm.shutdown()
  }

}
