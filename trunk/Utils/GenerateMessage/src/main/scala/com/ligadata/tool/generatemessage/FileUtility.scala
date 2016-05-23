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
//import com.ligadata.tool.generatemessage.ConfigBean
/**
  * Created by Yousef on 5/12/2016.
  */
case class configFile(delimiter: String, outputPath: String, saveMessage: String, nameSpace: String, partitionKey: String, primaryKey: String, timePartition: String, messageType: String, messageName: String)
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
    if(configInfo.delimiter.trim == "" && configInfo.outputPath.trim == "" && configInfo.saveMessage.trim == "" && configInfo.nameSpace.trim == ""
    &&configInfo.partitionKey.trim == "" && configInfo.primaryKey.trim == "" && configInfo.timePartition.trim == "" && configInfo.messageType.trim == "" && configInfo.messageName.trim == ""){
      logger.error("outputpath and delimiter cannot be null in config file")
      sys.exit(1)
    } else if(configInfo.outputPath.trim == "" || configInfo.delimiter.trim == ""){
      logger.error("outputpath and delimiter cannot be null in config file")
      sys.exit(1)
    } else {
      configBeanObj.outputPath_=(configInfo.outputPath)
      configBeanObj.delimiter_=(configInfo.delimiter)

      if(configInfo.saveMessage.trim != ""){
        if(dataTypeObj.isBoolean(configInfo.saveMessage)){
          configBeanObj.saveMessage_=(configInfo.saveMessage.toBoolean)
        } else{
          logger.error("the value for saveMessage should be true or false")
          sys.exit(1)
        }
      }

      if(configInfo.nameSpace.trim != ""){
        configBeanObj.nameSpace_=(configInfo.nameSpace.toString)
      }

      if(configInfo.messageName.trim != ""){
        configBeanObj.messageName_=(configInfo.messageName.toString)
      }

//      if(configInfo.partitionKey.trim != ""){
//        if(dataTypeObj.isBoolean(configInfo.partitionKey)){
//          configBeanObj.partitionKey_=(configInfo.partitionKey.toBoolean)
//        } else{
//          logger.error("the value for partitionKey should be true or false")
//          sys.exit(1)
//        }
//      }
//
//      if(configInfo.primaryKey.trim != ""){
//        if(dataTypeObj.isBoolean(configInfo.primaryKey)){
//          configBeanObj.primaryKey_=(configInfo.primaryKey.toBoolean)
//        } else{
//          logger.error("the value for primaryKey should be true or false")
//          sys.exit(1)
//        }
//      }
//
//      if(configInfo.timePartition.trim != ""){
//        if(dataTypeObj.isBoolean(configInfo.timePartition)){
//          configBeanObj.timePartition_=(configInfo.timePartition.toBoolean)
//        } else{
//          logger.error("the value for timePartition should be true or false")
//          sys.exit(1)
//        }
//      }

      if(configInfo.partitionKey.trim != ""){
        configBeanObj.partitionKey_=(configInfo.partitionKey.toString)
        configBeanObj.hasPartitionKey_=(true)
      }

      if(configInfo.primaryKey.trim != ""){
        configBeanObj.primaryKey_=(configInfo.primaryKey.toString)
        configBeanObj.hasPrimaryKey_=(true)
      }

      if(configInfo.timePartition.trim != ""){
        configBeanObj.timePartition_=(configInfo.timePartition.toString)
        configBeanObj.hasTimePartition_=(true)
      }

      if(configInfo.messageType.trim != ""){
        if(configInfo.messageType.equalsIgnoreCase("fixed")){
          configBeanObj.messageType_=(true)
        } else if(configInfo.messageType.equalsIgnoreCase("mapped")){
          configBeanObj.messageType_=(false)
        } else {
          logger.error("the value of massegeType should be fixed or mapped")
          sys.exit(1)
        }
      }
      return configBeanObj
    }
  }

  def writeToFile(json:JsonAST.JValue, filename: String): Unit = {
//        val json =
//          ("Meesage" ->
//            ("NameSpace" -> configObj.nameSpace) ~
//            ("Name" -> "")~
//            ("Verion" -> "00.01.00")~
//              ("Description" -> "")~
//              ("Fixed" -> configObj.messageType.toString)~
//              ("Persist" -> configObj.saveMessage)~
//              ("Feilds" ->
//                data.keys.map {
//                  key =>
//                    (
//                      ("Name" -> key) ~
//                        ("Type" -> "System.".concat(data(key))))
//                })~
//              ("partitionKey" -> "")~
//              ("primaryKey" -> "")~
//              ("TimePartitionInfo" -> "")
//            )
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
      val dateFormat = new SimpleDateFormat("ddMMyyyyhhmmss")
       filename = outputPath + "/message_" + dateFormat.format(new java.util.Date()) + ".json"
    }
    return filename
  }
}
