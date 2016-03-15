package com.ligadata.BasicCacheConcurrency

import java.io.IOException

import org.infinispan.Cache
import org.infinispan.manager.{DefaultCacheManager, EmbeddedCacheManager}

/**
  * Created by Saleh on 3/14/2016.
  */

object Node {
  def main(args: Array[String]) {
    val useXmlConfig: Boolean = false
    val cache: String = "dist"
    val nodeName: String = "test"

    new Node(useXmlConfig,cache,nodeName).run
  }
}

class Node(useXmlConfig: Boolean=false, cacheName: String, nodeName: String) {

  @volatile
  private var stop: Boolean = false

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  def run : Unit = {
    val cacheManager: EmbeddedCacheManager = createCacheManagerFromXml
    val cache: Cache[String, String] = cacheManager.getCache(cacheName)
    System.out.printf("Cache %s started on %s, cache members are now %s\n", cacheName, cacheManager.getAddress, cache.getAdvancedCache.getRpcManager.getMembers)

    cache.addListener(new LoggingListener)
  }

  @throws(classOf[IOException])
  private def createCacheManagerFromXml: EmbeddedCacheManager = {
    System.out.println("Starting a cache manager with an XML configuration")
    System.setProperty("nodeName", nodeName)
    return new DefaultCacheManager("infinispan.xml")
  }
}
