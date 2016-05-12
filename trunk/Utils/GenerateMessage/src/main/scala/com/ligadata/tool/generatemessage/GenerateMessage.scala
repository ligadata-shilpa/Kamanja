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
import scala.collection.mutable.HashMap
import java.io.File
import java.io.PrintWriter
import org.apache.logging.log4j._
import scala.io.Source.fromFile

trait LogTrait {
  val loggerName = this.getClass.getName()
  val logger = LogManager.getLogger(loggerName)
}


object GenerateMessage extends App with LogTrait{

  def usage: String = {
    """
Usage:  bash $KAMANJA_HOME/bin/GenerateMessage.sh --inputfile $KAMANJA_HOME/input/SampleApplication/data/file.csv --config $KAMANJA_HOME/config/file.json
    """
  }

  private type OptionMap = Map[Symbol, Any]

  private def nextOption(map: OptionMap, list: List[String]): OptionMap = {
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

    if (args.length == 0) {
      logger.error("Please pass the input file after --inputfile option and config file after --config operation")
      logger.warn(usage)
      sys.exit(1)
    }
    val options = nextOption(Map(), args.toList)

    val inputFile = options.getOrElse('inputfile, null).toString.trim
    if (inputFile == null || inputFile.toString().trim() == "") {
      logger.error("Please pass the input file after --inputfile option")
      logger.warn(usage)
      sys.exit(1)
    }

     val configFile = options.getOrElse('inputfile, null).toString.trim
     if (configFile == null || configFile.toString().trim() == "") {
       logger.error("Please pass the config file after --config option")
       logger.warn(usage)
       sys.exit(1)
     }

    var fileBen: FileUtility = new FileUtility()

     val inputFileExistFlag = fileBen.FileExist(inputFile) // check if input file exists
     val configFileExistFlag = fileBen.FileExist(configFile) // check if config file exists
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

     val inputFileContent = fileBen.ReadFile(inputFile) // read input file
     val configFileContent = fileBen.ReadFile(configFile) // read config file
     if (inputFileContent == null  || inputFileContent.size == 0) {
        logger.error("This file %s does not include data. Check your file please.".format(inputFile))
       logger.warn(usage)
        sys.exit(1)
      }
     if (configFileContent == null  || configFileContent.size == 0) {
       logger.error("This file %s does not include data. Check your file please.".format(configFile))
       logger.warn(usage)
       sys.exit(1)
     }

     val headerString = fileBen.ReadHeaderFile(inputFile)
     val headerFields = fileBen.SplitFile(inputFile, ",")
   }
}

