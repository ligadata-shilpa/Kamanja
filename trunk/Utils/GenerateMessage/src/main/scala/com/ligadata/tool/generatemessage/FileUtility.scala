package com.ligadata.tool.generatemessage

import com.ligadata.Exceptions.KamanjaException
import org.apache.commons.io.FilenameUtils
import org.json4s
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import scala.io.Source._
import com.ligadata.tool.generatemessage.ConfigBean
/**
  * Created by Yousef on 5/12/2016.
  */
case class configFile(delimiter: String, outputPath: String, saveMessage: String, nameSpace: String, partitionKey: String, primaryKey: String, timePartition: String)
class FileUtility  extends LogTrait{

  def FindFileExtension (filePath: String) : Boolean = {//This method used to check if the extension of file is json or not (return true if json and false otherwise)
    val ext = FilenameUtils.getExtension(filePath);
    if (ext.equalsIgnoreCase("json")){
      return true
    } else {
      return false
    }
  }

  def ParseFile(filePath: String): json4s.JValue ={//This method used to parse a config file (JSON format)
    try{
      val parsedFile = parse(filePath)
      return parsedFile
    } catch{
      case e: Exception => throw new KamanjaException(s"There is an error in the format of file \n ErrorMsg : ", e)
    }
  }

  def extractInfo(parsedValue: json4s.JValue): configFile={ //This method used to extract data from config file
    val extractedObj = parsedValue.extract[configFile]
    return extractedObj
  }

  def ReadFile(filePath: String): String ={//This method used to read a whole file
    return fromFile(filePath).mkString
  }

  def FileExist(filePath: String): Boolean={//This method used to check if file exists or not (return true if exists and false otherwise)
    return new java.io.File(filePath).exists
  }

  def ReadHeaderFile(filePath: String, ignoreLines: Int): String ={//This method used to read first line in file
    if (ignoreLines == 0) {
      val line = fromFile(filePath).getLines
      return line.next()
    } else {
      val line = fromFile(filePath).getLines.drop(ignoreLines)
      return line.next()
    }
  }

  def Countlines(filePath: String): Integer={ //return number of lines in file
    return fromFile(filePath).getLines.size
  }

  def SplitFile (filePath: String, delimiter: String): Array[String] = {//This method used to split file based on delimiter
    return filePath.split(delimiter)
  }

  def createConfigBeanObj(configInfo: configFile): ConfigBean={ //This method used to create a configObj
    var configBeanObj:ConfigBean = new ConfigBean()
    val dataTypeObj: DataTypeUtility = new DataTypeUtility()
    if(configInfo.delimiter == None && configInfo.outputPath == None && configInfo.saveMessage == None && configInfo.nameSpace == None
    &&configInfo.partitionKey == None && configInfo.primaryKey == None && configInfo.timePartition == None){
      logger.error("You should pass at least outputpath and delimiter in config file")
      sys.exit(1)
    } else if(configInfo.outputPath == None || configInfo.delimiter == None){
      logger.error("You should pass outputpath in config file")
      sys.exit(1)
    } else {
      configBeanObj.outputPath_=(configInfo.outputPath)
      configBeanObj.delimiter_=(configInfo.delimiter)

      if(configInfo.saveMessage != None){
        if(dataTypeObj.isBoolean(configInfo.saveMessage)){
          configBeanObj.saveMessage_=(true)
        } else{
          logger.error("the value for saveMessage should be true or false")
          sys.exit(1)
        }
      }

      if(configInfo.nameSpace != None){
        configBeanObj.nameSpace_=(configInfo.nameSpace.toString)
      }

      if(configInfo.partitionKey != None){
        if(dataTypeObj.isBoolean(configInfo.partitionKey)){
          configBeanObj.partitionKey_=(true)
        } else{
          logger.error("the value for partitionKey should be true or false")
          sys.exit(1)
        }
      }

      if(configInfo.primaryKey != None){
        if(dataTypeObj.isBoolean(configInfo.primaryKey)){
          configBeanObj.primaryKey_=(true)
        } else{
          logger.error("the value for primaryKey should be true or false")
          sys.exit(1)
        }
      }

      if(configInfo.timePartition != None){
        if(dataTypeObj.isBoolean(configInfo.timePartition)){
          configBeanObj.timePartition_=(true)
        } else{
          logger.error("the value for timePartition should be true or false")
          sys.exit(1)
        }
      }
      return configBeanObj
    }
  }
}
