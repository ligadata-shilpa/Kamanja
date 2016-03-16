package com.ligadata.BasicCacheConcurrency

import java.util._

import net.sf.ehcache
import net.sf.ehcache.{Cache, Element, Ehcache, CacheManager}
import net.sf.ehcache.event.CacheEventListener

/**
  * Created by Saleh on 3/14/2016.
  */
object NodeEhCache {
  def main(args: Array[String]) {

    val aclass = Class.forName("com.ligadata.BasicCacheConcurrency.MemoryDataCacheImp").newInstance
    val node = aclass.asInstanceOf[DataCache]

    node.start()
    var map = new HashMap[String,String]
    map.put("1","1")

    node.putInCache(map)

    System.out.println(node.getFromCache("1").toString)

    node.shutdwon()

  }
}




