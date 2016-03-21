package com.ligadata.BasicCacheConcurrency

import java.util.Properties

import net.sf.ehcache.config.{FactoryConfiguration, Configuration, CacheConfiguration}
import net.sf.ehcache.distribution.jgroups.{JGroupsCacheReplicatorFactory, JGroupsCacheManagerPeerProviderFactory}
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
  val MAXENTRIESLOCALHEAP:String="maxEntriesLocalHeap"
  val MAXENTRIESLOCALDISK:String="maxEntriesLocalDisk"
  val ETERNAL:String="eternal"
  val DISKSPOOLBUFFERSIZEMB:String="diskSpoolBufferSizeMB"
  val TIMETOIDLESECONDS:String="timeToIdleSeconds"
  val TIMETOLIVESECONDS:String="timeToLiveSeconds"
  val MEMORYSTOREEVICTIONPOLICY:String="memoryStoreEvictionPolicy"
  val TRANSACTIONALMODE:String="transactionalMode"
  val CLASS:String="class"
  val SEPARATOR:String="separator"
  val PEERCONFIG:String="peerconfig"
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
             replicatePuts=true
             replicateUpdates=true
             replicateUpdatesViaCopy=false
             replicateRemovals=true
             replicateAsynchronously=true

   */

  this.name(values.getOrElse(CacheCustomConfig.NAME,"Node"))
    .eternal(values.getOrElse(CacheCustomConfig.ETERNAL,"false").toBoolean)
    .maxEntriesLocalHeap(values.getOrElse(CacheCustomConfig.MAXENTRIESLOCALHEAP,"10000").toInt)
    .maxEntriesLocalDisk(values.getOrElse(CacheCustomConfig.MAXENTRIESLOCALDISK,"1000").toInt)
    .diskSpoolBufferSizeMB(values.getOrElse(CacheCustomConfig.DISKSPOOLBUFFERSIZEMB,"20").toInt)
    .timeToLiveSeconds(values.getOrElse(CacheCustomConfig.TIMETOLIVESECONDS,"600").toInt)
    .timeToIdleSeconds(values.getOrElse(CacheCustomConfig.TIMETOIDLESECONDS,"300").toInt)
    .memoryStoreEvictionPolicy(values.getOrElse(CacheCustomConfig.MEMORYSTOREEVICTIONPOLICY,"LFU"))
    .transactionalMode(values.getOrElse(CacheCustomConfig.TRANSACTIONALMODE,"off"))

  //ADD A PROVIDER DEFUALT JGROUPS
  factory.setClass(values.getOrElse(CacheCustomConfig.CLASS,"net.sf.ehcache.distribution.jgroups.JGroupsCacheManagerPeerProviderFactory"))
  factory.setPropertySeparator(values.getOrElse(CacheCustomConfig.SEPARATOR,"::"))
  factory.setProperties(values.getOrElse(CacheCustomConfig.PEERCONFIG,"channelName=EH_CACHE::file=jgroups.xml"));
  config.addCacheManagerPeerProviderFactory(factory)

  //LISTENER PROPERTIES
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
