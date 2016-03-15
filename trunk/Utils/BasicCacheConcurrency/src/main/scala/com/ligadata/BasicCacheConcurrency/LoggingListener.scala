package com.ligadata.BasicCacheConcurrency

import org.infinispan.notifications.Listener
import org.infinispan.notifications.cachelistener.annotation.{TopologyChanged, CacheEntryRemoved, CacheEntryModified, CacheEntryCreated}
import org.infinispan.notifications.cachelistener.event.{TopologyChangedEvent, CacheEntryRemovedEvent, CacheEntryModifiedEvent, CacheEntryCreatedEvent}
import org.jboss.logging.Logger


/**
  * Created by Saleh on 3/14/2016.
  */
@Listener class LoggingListener {
  private val log: Logger = Logger.getLogger(classOf[LoggingListener])

  @CacheEntryCreated def observeAdd(event: CacheEntryCreatedEvent[String, String]) {
    if (event.isPre) return
    log.infof("Cache entry %s added in cache %s", Array[AnyRef](event.getKey, event.getCache))
  }

  @CacheEntryModified def observeUpdate(event: CacheEntryModifiedEvent[String, String]) {
    if (event.isPre) return
    log.infof("Cache entry %s = %s modified in cache %s", Array[AnyRef](event.getKey, event.getValue, event.getCache))
  }

  @CacheEntryRemoved def observeRemove(event: CacheEntryRemovedEvent[String, String]) {
    if (event.isPre) return
    log.infof("Cache entry %s removed in cache %s", Array[AnyRef](new String(event.getKey),event.getCache))
  }

  @TopologyChanged def observeTopologyChange(event: TopologyChangedEvent[String, String]) {
    if (event.isPre) return
    log.infof("Cache %s topology changed, new membership is %s", Array[AnyRef](new String(event.getCache.getName),event.getConsistentHashAtEnd.getMembers))
  }
}

