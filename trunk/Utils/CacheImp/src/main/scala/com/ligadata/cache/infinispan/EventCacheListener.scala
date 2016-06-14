package com.ligadata.cache.infinispan

import com.ligadata.cache.{CacheCallbackData, CacheCallback}
import org.infinispan.notifications.Listener
import org.infinispan.notifications.cachelistener.annotation.{TopologyChanged, CacheEntryRemoved, CacheEntryModified, CacheEntryCreated}
import org.infinispan.notifications.cachelistener.event.{CacheEntryRemovedEvent, CacheEntryModifiedEvent, TopologyChangedEvent, CacheEntryCreatedEvent}

/**
  * Created by Saleh on 6/14/2016.
  */

@Listener
class EventCacheListener extends CacheCallback {

  @CacheEntryCreated
  def observeAdd(event: CacheEntryCreatedEvent[String, String]): Unit = {
    if (event.isPre())
      return;

    System.out.println("Cache entry "+event.getKey()+" added in cache "+event.getCache());
  }

  @CacheEntryModified
  def observeUpdate(event: CacheEntryModifiedEvent[String, String]) {
    if (event.isPre())
      return;

    System.out.println("Cache entry "+event.getKey()+" modified in cache "+event.getCache());
  }

  @CacheEntryRemoved
  def observeRemove(event: CacheEntryRemovedEvent[String, String]) {
    if (event.isPre())
      return;

    System.out.println("Cache entry "+event.getKey()+" removed in cache "+event.getCache());

  }

  @TopologyChanged
  def observeTopologyChange(event: TopologyChangedEvent[String, String]) {
    if (event.isPre())
      return;

    System.out.println("Cache entry "+event.getCache().getName()+" topology changed, new membership is "+event.getConsistentHashAtEnd().getMembers());
  }

  override def call(callbackData: CacheCallbackData): Unit = {
  }
}
