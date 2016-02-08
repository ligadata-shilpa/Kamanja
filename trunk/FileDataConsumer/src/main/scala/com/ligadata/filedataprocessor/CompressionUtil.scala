package com.ligadata.filedataprocessor

/**
  * Created by Yasser on 2/8/2016.
  */

import java.util.zip.GZIPInputStream
import java.io.{InputStream, IOException}


object CompressionType extends Enumeration {
  type CompressionType = Value
  val GZIP, BZIP2, LZO, UNKNOWN = Value
}

import CompressionType._

object CompressionUtil {

  def BZIP2_MAGIC = 0x685A42
  def LZO_MAGIC   = 0x4f5a4c
  def GZIP_MAGIC = GZIPInputStream.GZIP_MAGIC

  /**
    * checking the compression type by comparing magic numbers which is the head of the file
    * @param is : input stream object
    * @return CompressionType
    */
  def detectCompressionTypeByMagicNumbers(is : InputStream) : CompressionType = {
    //some types magic number is only two bytes and some are 3 or 4 bytes
    //if length is less than 2 bytes then the type is known (and probably the file is corrupt)

    val minlen = 2
    val maxlen = 4
    val buffer = new Array[Byte](maxlen)
    val readlen = is.read(buffer, 0, maxlen)
    if (readlen < minlen)
      return UNKNOWN

    //check for magic numbers with two bytes
    //for now only gzip
    val testGzipHead = (buffer(0) & 0xff) | ((buffer(1) << 8) & 0xff00)
    if(testGzipHead == GZIP_MAGIC)
      return GZIP

    if(readlen < 3)
      return UNKNOWN

    //check for magic numbers with three bytes
    //for now only bzip2
    val testBzip2Head = (buffer(0)& 0xff) | ((buffer(1) << 8) & 0xff00) | ((buffer(2) << 16) & 0xff0000)
    if(testBzip2Head == BZIP2_MAGIC)
      return BZIP2

    if(readlen < 4)
      return UNKNOWN

    //lzo magic number is 3 bytes but starting from the second byte
    val testLzoHead = (buffer(1) & 0xff) | ((buffer(2) << 8) & 0xff00) | ((buffer(3) << 16) & 0xff0000)
    if (testLzoHead == LZO_MAGIC)
      return LZO


    UNKNOWN
  }

  def testDetectCompressionTypeByExtension(filePath : String)  = {
    val compressionType = detectCompressionTypeByExtension(filePath)
    println(s"CompressionType for $filePath is $compressionType")
  }

  def detectCompressionTypeByExtension(filePath : String) : CompressionType = {

    val fileNameParts = filePath.split("\\.")
    if(fileNameParts.length < 2)
      return UNKNOWN

    val extension = fileNameParts(fileNameParts.length - 1).toLowerCase

    if(extension.equalsIgnoreCase("gzip") || extension.equalsIgnoreCase("gz"))
      return GZIP

    if(extension.equalsIgnoreCase("bz2"))
      return BZIP2

    if(extension.equalsIgnoreCase("lzo") || extension.equalsIgnoreCase("lzop"))
      return LZO

    UNKNOWN
  }
}
