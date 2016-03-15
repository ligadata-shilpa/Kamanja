package com.ligadata.BasicCacheConcurrency

import net.sf.ehcache.{Element, Ehcache}
import net.sf.ehcache.event.CacheEventListener

/**
  * Created by Saleh on 3/14/2016.
  */
class NotNullCacheEventListener extends CacheEventListener {
  override def notifyElementExpired(cache: Ehcache, element: Element): Unit = {System.out.println()}

  override def notifyElementEvicted(cache: Ehcache, element: Element): Unit = {System.out.println("put new evicted")}

  override def notifyRemoveAll(cache: Ehcache): Unit = {System.out.println()}

  override def notifyElementPut(cache: Ehcache, element: Element): Unit = {System.out.println("put new values")}

  override def dispose(): Unit = {System.out.println("put new dispose")}

  override def notifyElementRemoved(cache: Ehcache, element: Element): Unit = {System.out.println()}

  override def notifyElementUpdated(cache: Ehcache, element: Element): Unit = {System.out.println()}
}
