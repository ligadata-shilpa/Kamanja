package com.ligadata.filedataprocessor

import java.io.{FileNotFoundException, InputStream, IOException}
import java.util.zip.GZIPInputStream

import com.ligadata.Exceptions.StackTrace


/**
 * Created by Yasser on 12/6/2015.
 */


object FsType extends Enumeration {
  type FsType = Value
  val POSIX, HDFS, SFTP = Value
}

import FsType._

object FileHandler{
  /*def getFsType(path : String) : FsType = {
    if(path.toLowerCase.startsWith("sftp"))
      SFTP
    else if(path.toLowerCase.startsWith("hdfs"))
      HDFS
    else if(path.toLowerCase.startsWith("file"))
      POSIX
    else
      POSIX // in case path does not start with any protocol. ignoring other types like http
  }*/
  def getFsType(fs : String) : FsType = {
    if(fs == null)
      POSIX
    else if(fs.toLowerCase == "sftp")
      SFTP
    else if(fs.toLowerCase == "hdfs")
      HDFS
    else if(fs.toLowerCase == "file")
      POSIX
    else
      POSIX // default
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

  def isStreamCompressed(inputStream: InputStream): Boolean = {

    val maxlen = 2
    val buffer = new Array[Byte](maxlen)
    val readlen = inputStream.read(buffer, 0, maxlen)

    //inputStream.close() // Close before we really check and return the data

    if (readlen < 2)
      return false

    val b0: Int = buffer(0)
    val b1: Int = buffer(1)

    val head = (b0 & 0xff) | ((b1 << 8) & 0xff00)

    return (head == GZIPInputStream.GZIP_MAGIC);
  }
}