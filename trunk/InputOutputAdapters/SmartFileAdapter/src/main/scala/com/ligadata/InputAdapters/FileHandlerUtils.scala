package com.ligadata.InputAdapters

/**
 * Created by Yasser on 12/6/2015.
 */


object FsType extends Enumeration {
  type FsType = Value
  val POSIX, HDFS, SFTP = Value
}

import FsType._

object FileHandlerUtil{
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

