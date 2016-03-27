package com.ligadata.BasicCacheConcurrency


import net.sf.ehcache.{Element, Ehcache}
import net.sf.ehcache.event.CacheEventListener

/**
  * Created by Saleh on 3/27/2016.
  */

class EventCacheListener extends CacheEventListener {
  override def notifyElementExpired(cache: Ehcache, element: Element): Unit = {System.out.println("notifyElementExpired")}

  override def notifyElementEvicted(cache: Ehcache, element: Element): Unit = {System.out.println("notifyElementEvicted")}

  override def notifyRemoveAll(cache: Ehcache): Unit = {System.out.println("notifyRemoveAll")}

  override def notifyElementPut(cache: Ehcache, element: Element): Unit = {System.out.println("notifyElementPut " + new java.util.Date)}

  override def dispose(): Unit = {System.out.println("dispose")}

  override def notifyElementRemoved(cache: Ehcache, element: Element): Unit = {System.out.println("notifyElementRemoved")}

  override def notifyElementUpdated(cache: Ehcache, element: Element): Unit = {System.out.println("notifyElementUpdated")}
}