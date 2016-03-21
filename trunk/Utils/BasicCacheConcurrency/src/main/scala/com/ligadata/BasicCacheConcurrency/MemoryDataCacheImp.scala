package com.ligadata.BasicCacheConcurrency


import net.sf.ehcache.distribution.jgroups.JGroupsCacheReplicatorFactory
import net.sf.ehcache.event.CacheEventListener
import net.sf.ehcache.{Element, Cache, CacheManager}


import scala.collection.JavaConverters._

/**
  * Created by Saleh on 3/15/2016.
  */

class MemoryDataCacheImp extends DataCache{

  var cm:CacheManager = null
  var cache:Cache = null
  var cacheConfig:CacheCustomConfig = null

  override def init(jsonString:String): Unit = {

    cacheConfig = new CacheCustomConfig(jsonString)

  }

  override def start(): Unit = {
    cm = CacheManager.create(cacheConfig.getConfiguration())
    cm.addCache(new Cache(cacheConfig))
    cache = cm.getCache(cacheConfig.getName)
    cache.getCacheEventNotificationService.registerListener(cacheConfig.getListener())
  }

  override def shutdown(): Unit = {
    cm.shutdown()
  }

  override def put(key: String, value: scala.Any): Unit = {
    cache.put(new Element(key,value))
  }

  override def get(key: String): AnyRef = {
    if(cache.isKeyInCache(key)){

      val ele:Element = cache.get(key)

      return ele.getObjectValue
    }else{
      System.out.println("get data from SSD");

      return ""
    }

  }

  override def get(keys: Array[String]): java.util.Map[String, AnyRef] = {
    val map = new java.util.HashMap[String, AnyRef]
    keys.foreach(str => map.put(str,get(str)))

    map
  }

  override def put(map: java.util.Map[_, _]): Unit = {
    val scalaMap = map.asScala
    scalaMap.foreach {keyVal => cache.put(new Element(keyVal._1,keyVal._2))}
  }
}
