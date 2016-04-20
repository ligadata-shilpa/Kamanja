package com.ligadata.OutputAdapters

trait SmartFile {
  def open: String
  //def write
  //def close
}

object SmartFile {
  private class FsFile extends SmartFile {
    override def open1:String = { return "1"}
    override def open2:String = {return "2"}
    override def open = if(1==2) open1 else open2
  }

  private class HdfsFile extends SmartFile {
    override def open:String = {return ""}
  }
  
  def apply(s: String): SmartFile = {
    if ("hdfs".equalsIgnoreCase(s)) 
      return new HdfsFile
    else 
      return new FsFile
  }
}
