package com.ligadata.tool.generatemessage

import java.io.PrintWriter
import java.text.SimpleDateFormat

import com.ligadata.Exceptions.KamanjaException
import org.apache.commons.io.FilenameUtils
import org.json4s
import org.json4s.{JsonAST, DefaultFormats}
import org.json4s.JsonDSL._
//import org.json4s.jackson.JsonMethods._
import org.json4s.native.JsonMethods._
import scala.collection.immutable.Map
import scala.io.Source._
/**
  * Created by Yousef on 5/12/2016.
  */
case class configFile(delimiter: Option[String], outputPath: Option[String], saveMessage: Option[String], nameSpace: Option[String],
                      partitionKey: Option[String], primaryKey: Option[String], timePartition: Option[String], messageType: Option[String],
                      messageName: Option[String])

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
  implicit val formats = DefaultFormats
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
    if(configInfo.delimiter.get == None && configInfo.outputPath.get == None){
      logger.error("you should pass at least outputpath and delimiter in config file")
      sys.exit(1)
    } else if(configInfo.outputPath.get.trim == "" || configInfo.delimiter.get.trim == ""){
      logger.error("outputpath and delimiter cannot be null in config file")
      sys.exit(1)
    } else {
      configBeanObj.outputPath_=(configInfo.outputPath.get)
      configBeanObj.delimiter_=(configInfo.delimiter.get)

      if(dataTypeObj.isBoolean(configInfo.saveMessage.getOrElse("false"))){
        configBeanObj.saveMessage_=(configInfo.saveMessage.get.toBoolean)
      } else{
        logger.error("the value for saveMessage should be true or false")
        sys.exit(1)
      }

      configBeanObj.nameSpace_=(configInfo.nameSpace.getOrElse("com.message"))

      configBeanObj.messageName_=(configInfo.messageName.getOrElse("testmessage"))

      configBeanObj.partitionKey_=(configInfo.partitionKey.getOrElse("false"))
      if(configBeanObj.partitionKey == true) configBeanObj.hasPartitionKey_=(true) else configBeanObj.hasPartitionKey_=(false)

      configBeanObj.primaryKey_=(configInfo.primaryKey.getOrElse("false"))
      if(configBeanObj.primaryKey == true) configBeanObj.hasPrimaryKey_=(true) else configBeanObj.hasPrimaryKey_=(false)

      configBeanObj.timePartition_=(configInfo.timePartition.getOrElse("false"))
      if(configBeanObj.timePartition == true) configBeanObj.hasTimePartition_=(true) else configBeanObj.hasTimePartition_=(false)

        if(configInfo.messageType.get.equalsIgnoreCase("fixed")){
          configBeanObj.messageType_=(true)
        } else if(configInfo.messageType.get.equalsIgnoreCase("mapped")){
          configBeanObj.messageType_=(false)
        } else {
          logger.error("the value of massegeType should be fixed or mapped")
          sys.exit(1)
        }

      return configBeanObj
    }
  }

  def writeToFile(json:JsonAST.JValue, filename: String): Unit = {
        new PrintWriter(filename) {
          write(pretty(render(json)));
          close
        }
  }

  def CreateFileName(outputPath: String): String={
    var filename = ""
    if (outputPath == null) {
      logger.error("output path should not be null")
      sys.exit(1)
    } else if(!FileExist(outputPath)) {
      logger.error("the output path does not exists")
      sys.exit(1)
    } else {
      val dateFormat = new SimpleDateFormat("yyyyMMddhhmmss")
       filename = outputPath + "/message_" + dateFormat.format(new java.util.Date()) + ".json"
    }
    return filename
  }
}
