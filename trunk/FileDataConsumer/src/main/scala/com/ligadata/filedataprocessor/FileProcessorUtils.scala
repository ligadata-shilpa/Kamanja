package com.ligadata.filedataprocessor

/**
  * Created by Yasser on 1/25/2016.
  */
object FileProcessorUtils {

  def toCharArray(bytes : Array[Byte]) : Array[Char] = {
    if(bytes == null)
      return null

    bytes.map(b => b.toChar)
  }
}
