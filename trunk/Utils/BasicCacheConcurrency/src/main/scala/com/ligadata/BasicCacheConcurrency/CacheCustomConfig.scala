package com.ligadata.BasicCacheConcurrency

import java.util.Properties

import net.sf.ehcache.config.{FactoryConfiguration, Configuration, CacheConfiguration}
import net.sf.ehcache.distribution.jgroups.{JGroupsCacheReplicatorFactory, JGroupsCacheManagerPeerProviderFactory}
import net.sf.ehcache.event.CacheEventListener


/**
  * Created by Saleh on 3/21/2016.
  */

object CacheCustomConfig{
  val REPLICATE_PUTS:String = "replicatePuts";
  val REPLICATE_UPDATES:String = "replicateUpdates";
  val REPLICATE_UPDATES_VIA_COPY:String = "replicateUpdatesViaCopy";
  val REPLICATE_REMOVALS:String = "replicateRemovals";
  val REPLICATE_ASYNCHRONOUSLY:String = "replicateAsynchronously";
}

class CacheCustomConfig(jsonString:String) extends CacheConfiguration{
  private val config:Configuration = new Configuration
  private val factory:FactoryConfiguration[Nothing] = new FactoryConfiguration
  private val properties:Properties = new Properties
  private val json = org.json4s.jackson.JsonMethods.parse(jsonString)
  private val values = json.values.asInstanceOf[Map[String, String]]

  /*
             name="Node"
             maxEntriesLocalHeap="10000"
             maxEntriesLocalDisk="1000"
             eternal="false"
             diskSpoolBufferSizeMB="20"
             timeToIdleSeconds="300"
             timeToLiveSeconds="600"
             memoryStoreEvictionPolicy="LFU"
             transactionalMode="off"
             class="net.sf.ehcache.distribution.jgroups.JGroupsCacheManagerPeerProviderFactory"
             separator="::"
             peerconfig="channelName=EH_CACHE::file=jgroups.xml"

   */

  this.name(values.getOrElse("name","Node"))
    .eternal(values.getOrElse("eternal","false").toBoolean)
    .maxEntriesLocalHeap(values.getOrElse("maxEntriesLocalHeap","10000").toInt)
    .maxEntriesLocalDisk(values.getOrElse("maxEntriesLocalDisk","1000").toInt)
    .diskSpoolBufferSizeMB(values.getOrElse("diskSpoolBufferSizeMB","20").toInt)
    .timeToLiveSeconds(values.getOrElse("timeToLiveSeconds","600").toInt)
    .timeToIdleSeconds(values.getOrElse("timeToIdleSeconds","300").toInt)
    .memoryStoreEvictionPolicy(values.getOrElse("memoryStoreEvictionPolicy","LFU"))
    .transactionalMode(values.getOrElse("transactionalMode","off"))

  factory.setClass(values.getOrElse("class","net.sf.ehcache.distribution.jgroups.JGroupsCacheManagerPeerProviderFactory"))
  factory.setPropertySeparator(values.getOrElse("separator","::"))
  factory.setProperties(values.getOrElse("peerconfig","channelName=EH_CACHE::file=jgroups.xml"));
  config.addCacheManagerPeerProviderFactory(factory)

  properties.setProperty(CacheCustomConfig.REPLICATE_PUTS,(values.getOrElse(CacheCustomConfig.REPLICATE_PUTS,"true")).toString)
  properties.setProperty(CacheCustomConfig.REPLICATE_UPDATES,(values.getOrElse(CacheCustomConfig.REPLICATE_UPDATES,"true")).toString)
  properties.setProperty(CacheCustomConfig.REPLICATE_UPDATES_VIA_COPY,(values.getOrElse(CacheCustomConfig.REPLICATE_UPDATES_VIA_COPY,"false")).toString)
  properties.setProperty(CacheCustomConfig.REPLICATE_REMOVALS,(values.getOrElse(CacheCustomConfig.REPLICATE_REMOVALS,"true")).toString)
  properties.setProperty(CacheCustomConfig.REPLICATE_ASYNCHRONOUSLY,(values.getOrElse(CacheCustomConfig.REPLICATE_ASYNCHRONOUSLY,"true")).toString)

  def  getConfiguration() : Configuration = {
    return config
  }

  def  getListener() : CacheEventListener = {
    return (new JGroupsCacheReplicatorFactory).createCacheEventListener(properties)
  }
}
