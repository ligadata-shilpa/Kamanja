package com.ligadata.filedataprocessor

import java.io.IOException

/**
 * Created by Yasser on 12/6/2015.
 */

object FileChangeType extends Enumeration {
  type FileChangeType = Value
  val AlreadyExisting, New, Modified = Value
}

trait FileHandler{
  def fullPath : String

  @throws(classOf[IOException])
  def openForRead(): Unit
  @throws(classOf[IOException])
  def read(buf : Array[Byte], length : Int) : Int
  @throws(classOf[IOException])
  def close(): Unit
  @throws(classOf[IOException])
  def moveTo(newPath : String) : Boolean
  @throws(classOf[IOException])
  def delete() : Boolean
}