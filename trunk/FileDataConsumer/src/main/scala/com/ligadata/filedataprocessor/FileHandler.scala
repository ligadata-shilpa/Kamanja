package com.ligadata.filedataprocessor

import java.io.IOException



/**
 * Created by Yasser on 12/6/2015.
 */


object FsType extends Enumeration {
  type FsType = Value
  val POSIX, HDFS, SFTP = Value
}

import FsType._

object FileHandler{
  def getFsType(path : String) : FsType = {
    if(path.toLowerCase.startsWith("sftp"))
      SFTP
    else if(path.toLowerCase.startsWith("hdfs"))
      HDFS
    else if(path.toLowerCase.startsWith("file"))
      POSIX
    else
      POSIX // in case path does not start with any protocol. ignoring other types like http
  }
}

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

  @throws(classOf[IOException])
  def length : Long

  @throws(classOf[IOException])
  def lastModified : Long
}