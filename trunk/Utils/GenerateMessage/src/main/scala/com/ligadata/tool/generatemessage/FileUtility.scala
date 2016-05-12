package com.ligadata.tool.generatemessage

import com.ligadata.Exceptions.KamanjaException
import org.apache.commons.io.FilenameUtils
import org.json4s
import org.json4s.native.JsonMethods._
import scala.io.Source._

/**
  * Created by Yousef on 5/12/2016.
  */
class FileUtility  extends LogTrait{

  def FindFileExtension (filePath: String) : Boolean = {
    val ext = FilenameUtils.getExtension(filePath);
    if (ext.equalsIgnoreCase("json")){
      return true
    } else {
      return false
    }
  }

  def ParseFile(filePath: String): json4s.JValue ={
    try{
      val parsedFile = parse(filePath)
      return parsedFile
    } catch{
      case e: Exception => throw new KamanjaException(s"There is an error in the format of file \n ErrorMsg : ", e)
    }
  }

  def ReadFile(filePath: String): String ={
    return fromFile(filePath).mkString // read file (JSON file)
  }

  def FileExist(filePath: String): Boolean={
    return new java.io.File(filePath).exists
  }

  def ReadHeaderFile(filePath: String): String ={
    val line = fromFile(filePath).getLines
    return line.next()
  }

  def SplitFile (filePath: String, delimiter: String): Array[String] = {
    return filePath.split(delimiter)
  }

}
