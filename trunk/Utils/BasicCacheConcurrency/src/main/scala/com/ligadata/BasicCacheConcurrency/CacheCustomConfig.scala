package com.ligadata.BasicCacheConcurrency

import java.util.Properties

import net.sf.ehcache.bootstrap.BootstrapCacheLoader
import net.sf.ehcache.config.{MemoryUnit, FactoryConfiguration, Configuration, CacheConfiguration}
import net.sf.ehcache.distribution.jgroups.{JGroupsBootstrapCacheLoaderFactory, JGroupsCacheReplicatorFactory, JGroupsCacheManagerPeerProviderFactory}
import net.sf.ehcache.event.CacheEventListener


/**
  * Created by Saleh on 3/21/2016.
  */

object CacheCustomConfig{
  val REPLICATE_PUTS:String = "replicatePuts"
  val REPLICATE_UPDATES:String = "replicateUpdates"
  val REPLICATE_UPDATES_VIA_COPY:String = "replicateUpdatesViaCopy"
  val REPLICATE_REMOVALS:String = "replicateRemovals"
  val REPLICATE_ASYNCHRONOUSLY:String = "replicateAsynchronously"
  val NAME:String="name"
  val MAXBYTESLOCALHEAP:String="maxBytesLocalHeap"
  val MAXBYTESLOCALDISK:String="maxBytesLocalDisk"
  val ETERNAL:String="eternal"
  val DISKSPOOLBUFFERSIZEMB:String="diskSpoolBufferSizeMB"
  val TIMETOIDLESECONDS:String="timeToIdleSeconds"
  val TIMETOLIVESECONDS:String="timeToLiveSeconds"
  val MEMORYSTOREEVICTIONPOLICY:String="memoryStoreEvictionPolicy"
  val TRANSACTIONALMODE:String="transactionalMode"
  val CLASS:String="class"
  val SEPARATOR:String="separator"
  val PEERCONFIG:String="peerconfig"
  val BOOTSTRAPASYNCHRONOUSLY:String="bootstrapAsynchronously"
}

class CacheCustomConfig(jsonString:String) extends CacheConfiguration{
  private val config:Configuration = new Configuration
  private val factory:FactoryConfiguration[Nothing] = new FactoryConfiguration
  private val properties:Properties = new Properties
  private val propertiesBootStrap:Properties = new Properties
  private val json = org.json4s.jackson.JsonMethods.parse(jsonString)
  private val values = json.values.asInstanceOf[Map[String, String]]

  /*
             name="Node"
             maxBytesLocalHeap="10000"
             maxBytesLocalDisk="1000"
             eternal="false"
             diskSpoolBufferSizeMB="20"
             timeToIdleSeconds="300"
             timeToLiveSeconds="600"
             memoryStoreEvictionPolicy="LFU"
             transactionalMode="off"
             class="net.sf.ehcache.distribution.jgroups.JGroupsCacheManagerPeerProviderFactory"
             separator="::"
             peerconfig="channelName=EH_CACHE::file=jgroups_upd.xml"
             replicatePuts=true
             replicateUpdates=true
             replicateUpdatesViaCopy=false
             replicateRemovals=true
             replicateAsynchronously=true
             bootstrapAsynchronously=false

   */

  this.name(values.getOrElse(CacheCustomConfig.NAME,"Node"))
    .eternal(values.getOrElse(CacheCustomConfig.ETERNAL,"false").toBoolean)
    .maxBytesLocalHeap(values.getOrElse(CacheCustomConfig.MAXBYTESLOCALHEAP,"10000").toLong,MemoryUnit.BYTES)
    .maxBytesLocalDisk(values.getOrElse(CacheCustomConfig.MAXBYTESLOCALDISK,"1000").toLong,MemoryUnit.BYTES)
    .diskSpoolBufferSizeMB(values.getOrElse(CacheCustomConfig.DISKSPOOLBUFFERSIZEMB,"20").toInt)
    .timeToLiveSeconds(values.getOrElse(CacheCustomConfig.TIMETOLIVESECONDS,"600").toInt)
    .timeToIdleSeconds(values.getOrElse(CacheCustomConfig.TIMETOIDLESECONDS,"300").toInt)
    .memoryStoreEvictionPolicy(values.getOrElse(CacheCustomConfig.MEMORYSTOREEVICTIONPOLICY,"LFU"))
    .transactionalMode(values.getOrElse(CacheCustomConfig.TRANSACTIONALMODE,"off"))

  //ADD A PROVIDER DEFUALT JGROUPS
  factory.setClass(values.getOrElse(CacheCustomConfig.CLASS,"net.sf.ehcache.distribution.jgroups.JGroupsCacheManagerPeerProviderFactory"))
  factory.setPropertySeparator(values.getOrElse(CacheCustomConfig.SEPARATOR,"::"))
  factory.setProperties(values.getOrElse(CacheCustomConfig.PEERCONFIG,"channelName=EH_CACHE::file=jgroups_upd.xml"));
  config.addCacheManagerPeerProviderFactory(factory)

  //LISTENER PROPERTIES
  properties.setProperty(CacheCustomConfig.REPLICATE_PUTS,(values.getOrElse(CacheCustomConfig.REPLICATE_PUTS,"true")).toString)
  properties.setProperty(CacheCustomConfig.REPLICATE_UPDATES,(values.getOrElse(CacheCustomConfig.REPLICATE_UPDATES,"true")).toString)
  properties.setProperty(CacheCustomConfig.REPLICATE_UPDATES_VIA_COPY,(values.getOrElse(CacheCustomConfig.REPLICATE_UPDATES_VIA_COPY,"false")).toString)
  properties.setProperty(CacheCustomConfig.REPLICATE_REMOVALS,(values.getOrElse(CacheCustomConfig.REPLICATE_REMOVALS,"true")).toString)
  properties.setProperty(CacheCustomConfig.REPLICATE_ASYNCHRONOUSLY,(values.getOrElse(CacheCustomConfig.REPLICATE_ASYNCHRONOUSLY,"true")).toString)

  //ADD BOOTSTRAP PROPERTIES
  propertiesBootStrap.setProperty(CacheCustomConfig.BOOTSTRAPASYNCHRONOUSLY,(values.getOrElse(CacheCustomConfig.BOOTSTRAPASYNCHRONOUSLY,"false")).toString)

  def  getConfiguration() : Configuration = {
    return config
  }

  def  getListener() : CacheEventListener = {
    return (new JGroupsCacheReplicatorFactory).createCacheEventListener(properties)
  }

  def  getBootStrap() : BootstrapCacheLoader = {
    return (new JGroupsBootstrapCacheLoaderFactory).createBootstrapCacheLoader(propertiesBootStrap)
  }
}
