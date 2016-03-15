package com.ligadata.BasicCacheConcurrency

import net.sf.ehcache
import net.sf.ehcache.{Cache, Element, Ehcache, CacheManager}
import net.sf.ehcache.event.CacheEventListener

/**
  * Created by Saleh on 3/14/2016.
  */
object NodeEhCache {
  def main(args: Array[String]) {
    val node = new NodeEhCache

    node.run("C:\\Users\\Saleh\\Documents\\GitHub\\Kamanja\\trunk\\BasicCacheConcurrency\\src\\main\\resources\\ehcache.xml")
    val cache = node.getCache("userCache")

    cache.getCacheEventNotificationService.registerListener(new NotNullCacheEventListener)

    cache.put(new Element("1","1"))

    val ele:Element = cache.get("1")
    System.out.println(ele.getObjectValue.toString);
  }
}

class NodeEhCache{

  var cm:CacheManager = null

  def run(xmlPath:String) : Unit = {
     cm = CacheManager.newInstance(xmlPath)
  }

  def getCache(cacheName:String) : Cache ={
    val cache = cm.getCache(cacheName)

    cache
  }

  def shutdown() : Unit={
    cm.shutdown()
  }
}



