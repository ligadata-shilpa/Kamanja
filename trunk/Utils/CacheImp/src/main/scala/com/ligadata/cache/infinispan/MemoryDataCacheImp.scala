package com.ligadata.cache.infinispan

import java.util

import com.ligadata.cache._
import net.sf.ehcache.config.Configuration
import org.infinispan.configuration.cache.{CacheMode, ConfigurationBuilder}
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.Cache
import org.infinispan.tree.{Node, Fqn, TreeCacheFactory, TreeCache}
;

/**
  * Created by Saleh on 6/9/2016.
  */
class MemoryDataCacheImp extends DataCache {

  var cacheManager: DefaultCacheManager = null
  var config: CacheCustomConfig = null
  var cache: Cache[String, Any] = null
  var listenCallback: CacheCallback = null
  var treeCache: TreeCache[String, Any] = null

  override def init(jsonString: String, listenCallback: CacheCallback): Unit = {
    config = new CacheCustomConfig(new Config(jsonString), cacheManager)
    cacheManager = config.getDefaultCacheManager()
    this.listenCallback = listenCallback
  }

  override def start(): Unit = {
    cache = cacheManager.getCache(config.getcacheName());
    cache.addListener(listenCallback)
    treeCache = new TreeCacheFactory().createTreeCache(cache);
  }

  override def shutdown(): Unit = {
    cacheManager.stop()
  }

  override def put(key: String, value: scala.Any): Unit = {
    cache.put(key, value)
  }

  override def get(key: String): AnyRef = {
    if (cache.containsKey(key)) {

      val obj = cache.get(key).asInstanceOf[AnyRef]

      return obj
    } else {
      return ""
    }
  }

  override def isKeyInCache(key: String): Boolean = (cache != null && cache.containsKey(key))

  override def get(keys: Array[String]): java.util.Map[String, AnyRef] = {
    val map = new java.util.HashMap[String, AnyRef]
    keys.foreach(str => map.put(str, get(str)))

    map
  }

  override def put(map: java.util.Map[_, _]): Unit = {
    val map = new java.util.HashMap[String, AnyRef]
    val keys = getKeys()
    if (keys != null) {
      keys.foreach(str => map.put(str, get(str)))
    }
    map
  }

  override def getAll(): java.util.Map[String, AnyRef] = {
    val map = new java.util.HashMap[String, AnyRef]
    val keys = getKeys()
    if (keys != null) {
      keys.foreach(str => map.put(str, get(str)))
    }
    map
  }

  override def getKeys(): Array[String] = {
    val size: Int = cache.keySet().size()
    val array = new Array[String](size)
    cache.keySet().toArray[String](array)

    array
  }

  override def put(containerName: String, timestamp: String, key: String, value: scala.Any): Unit = {
    if (!containerName.equals(null) && !"".equals(containerName)) {
      if (!timestamp.equals(null) && !"".equals(timestamp)) {
        if (!key.equals(null) && !"".equals(key)) {
          val fqn: Fqn = Fqn.fromElements(containerName, timestamp);
          treeCache.put(fqn, key, value);
        }

      }
    }
  }

  override def get(containerName: String, map:java.util.Map[String, java.util.Map[String, AnyRef]]): Unit = {
    if (!containerName.equals(null) && !"".equals(containerName)) {
      val containerNameCheck: Fqn = Fqn.fromElements(containerName)
      if (treeCache.exists(containerNameCheck)) {
        val allTimeStamp: util.Iterator[String] = treeCache.getKeys(containerNameCheck).iterator()
        while (allTimeStamp.hasNext) {
          val timeStamp: String = allTimeStamp.next()
          val timestampFqn: Fqn = Fqn.fromElements(containerName, timeStamp)
          map.put(timeStamp, treeCache.getData(timestampFqn).asInstanceOf[java.util.Map[String, AnyRef]])
        }
      }
    }
  }

  override def get(containerName: String, timestamp: String): util.Map[String, AnyRef] = {
    if (!containerName.equals(null) && !"".equals(containerName)) {
      if (!timestamp.equals(null) && !"".equals(timestamp)) {
        val timestampCheck: Fqn = Fqn.fromElements(containerName, timestamp);
        if (treeCache.exists(timestampCheck)) {
          return treeCache.getData(timestampCheck).asInstanceOf[util.Map[String, AnyRef]]
        }
      }
    }
    null
  }

  override def get(containerName: String, timestamp: String, key: String): AnyRef = {
    if (!containerName.equals(null) && !"".equals(containerName)) {
      if (!timestamp.equals(null) && !"".equals(timestamp)) {
        if (!key.equals(null) && !"".equals(key)) {
          val keyCheck: Fqn = Fqn.fromElements(containerName, timestamp, key);
          if (treeCache.exists(keyCheck)) {
            return treeCache.get(keyCheck, key).asInstanceOf[AnyRef];
          }
        }
      }
    }
    null
  }

  override def del(containerName: String): Unit = {
    if (!containerName.equals(null) && !"".equals(containerName)) {
      val fqn: Fqn = Fqn.fromElements(containerName);
      treeCache.removeNode(fqn)
    }
  }

  override def del(containerName: String, timestamp: String): Unit = {
    if (!containerName.equals(null) && !"".equals(containerName)) {
      if (!timestamp.equals(null) && !"".equals(timestamp)) {
        val fqn: Fqn = Fqn.fromElements(containerName, timestamp);
        treeCache.removeNode(fqn)
      }
    }
  }

  override def del(containerName: String, timestamp: String, key: String): Unit = {
    if (!containerName.equals(null) && !"".equals(containerName)) {
      if (!timestamp.equals(null) && !"".equals(timestamp)) {
        if (!key.equals(null) && !"".equals(key)) {
          val fqn: Fqn = Fqn.fromElements(containerName, timestamp, key);
          treeCache.removeNode(fqn)
        }

      }
    }
  }
}

