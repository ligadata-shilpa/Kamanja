/*
 * Copyright 2015 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ligadata.tool.generatemessage

import org.apache.commons.io.FilenameUtils
import org.json4s._
import org.json4s.JsonDSL._
import com.ligadata.Exceptions._
import org.json4s.native.JsonMethods._
import scala.collection.immutable.HashMap
import java.io.File
import java.io.PrintWriter
import org.apache.logging.log4j._
import scala.io.Source.fromFile

trait LogTrait {
  val loggerName = this.getClass.getName()
  val logger = LogManager.getLogger(loggerName)
}


object GenerateMessage extends App with LogTrait{

  def usage: String = { //This method used to tell user how he can use the tool
    """
Usage:  bash $KAMANJA_HOME/bin/GenerateMessage.sh --inputfile $KAMANJA_HOME/input/SampleApplication/data/file.csv --config $KAMANJA_HOME/config/file.json
    """
  }

  private type OptionMap = Map[Symbol, Any]

  private def nextOption(map: OptionMap, list: List[String]): OptionMap = {//This method used to parse the input parameter for tool
    def isSwitch(s: String) = (s.charAt(0) == '-')
    list match {
      case Nil => map
      case "--inputfile" :: value :: tail =>
        nextOption(map ++ Map('inputfile -> value), tail)
      case "--config" :: value :: tail =>
        nextOption(map ++ Map('config -> value), tail)
      case option :: tail => {
        logger.error("Unknown option " + option)
        logger.warn(usage)
        sys.exit(1)
      }
    }
  }

   override def main(args: Array[String]) {

    logger.debug("GenerateMessage.main begins")

    if (args.length == 0) { //Check if user did not pass parameters
      logger.error("Please pass the input file after --inputfile option and config file after --config operation")
      logger.warn(usage)
      sys.exit(1)
    }
    val options = nextOption(Map(), args.toList)

    val inputFile = options.getOrElse('inputfile, null).toString.trim //Read inputFile value from parsed parameters
    if (inputFile == null || inputFile.toString().trim() == "") { //check if inputFile passed or not
      logger.error("Please pass the input file after --inputfile option")
      logger.warn(usage)
      sys.exit(1)
    }

     val configFile = options.getOrElse('config, null).toString.trim //Raad config value from parsed parameters
     if (configFile == null || configFile.toString().trim() == "") { //check if config passed or not
       logger.error("Please pass the config file after --config option")
       logger.warn(usage)
       sys.exit(1)
     }

     val fileBean: FileUtility = new FileUtility()
     val dataTypeObj: DataTypeUtility = new DataTypeUtility()

     val inputFileExistFlag = fileBean.FileExist(inputFile) // check if inputFile path exists
     val configFileExistFlag = fileBean.FileExist(configFile) // check if config file path exists
     if (inputFileExistFlag == false){
       logger.error("This file %s does not exists".format(inputFileExistFlag))
       logger.warn(usage)
       sys.exit(1)
     }
     if (configFileExistFlag == false){
       logger.error("This file %s does not exists".format(configFileExistFlag))
       logger.warn(usage)
       sys.exit(1)
     }

     val inputFileContent = fileBean.ReadFile(inputFile) // read inputFile contents
     val configFileContent = fileBean.ReadFile(configFile) // read config file contents
     if (inputFileContent == null  || inputFileContent.size == 0) { //check if inputFile includes data
        logger.error("This file %s does not include data. Check your file please.".format(inputFile))
       logger.warn(usage)
        sys.exit(1)
      }
     if (configFileContent == null  || configFileContent.size == 0) { // check if config file includes data
       logger.error("This file %s does not include data. Check your file please.".format(configFile))
       logger.warn(usage)
       sys.exit(1)
     }

     val parsedConfig = fileBean.ParseFile(configFileContent) //Parse config file
     val extractedInfo = fileBean.extractInfo(parsedConfig) //Extract information from parsed file
     val configBeanObj = fileBean.createConfigBeanObj(extractedInfo)// create a config object that store the result from extracting config file
     val fileSize = fileBean.Countlines(inputFile) // Find number of lines in file
     val headerString = fileBean.ReadHeaderFile(inputFile, 0) //read the header line for inputFile
     val headerFields = fileBean.SplitFile(headerString, configBeanObj.delimiter) //split the header line based on delimiter

     var feildsString = Map[String, String]()
     for(itemIndex <- 0 to headerFields.length-1) {
       if (dataTypeObj.isAllDigits(headerFields(itemIndex))) {
         //Check if all character are digits
         logger.error("This %s file does not include header".format(inputFile))
         sys.exit(1)
       }
       var previousType = dataTypeObj.FindFinalType(fileSize, itemIndex, inputFile,configBeanObj.delimiter)
       feildsString = feildsString + (headerFields(itemIndex) -> previousType)
     }
     val jsonBean: JsonUtility = new JsonUtility()
     val fileName = fileBean.CreateFileName(configBeanObj.outputPath) // create name for output file
     val json = jsonBean.FinalJsonString(feildsString,configBeanObj) // create json string
     fileBean.writeToFile(json,fileName) // write json string to output file
     logger.info("message created successfully")
     logger.info("you can find the file in this path %s".format(fileName))
     println("[RESULT] - message created successfully")
     println("[RESULT] - you can find the file in this path %s".format(fileName))
   }
}

