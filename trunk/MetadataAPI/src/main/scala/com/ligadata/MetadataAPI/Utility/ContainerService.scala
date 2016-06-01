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

package com.ligadata.MetadataAPI.Utility

import java.io.File

import com.ligadata.MetadataAPI.{MetadataAPIImpl,ApiResult,ErrorCodeConstants}

import scala.io.Source

import org.apache.logging.log4j._

import scala.io._

/**
 * Created by dhaval on 8/7/15.
 */
object ContainerService {
  private val userid: Option[String] = Some("kamanja")
  val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)
  // 646 - 676 Change begins - replase MetadataAPIImpl
  val metadataAPI = MetadataAPIImpl.getMetadataAPI
  // 646 - 676 Chagne ends


  def addContainer(input: String, tid: Option[String] = None): String ={
    var response = ""
    var containerFileDir: String = ""

    //val gitMsgFile = "https://raw.githubusercontent.com/ligadata-dhaval/Kamanja/master/HelloWorld_Msg_Def.json"
    var chosen: String = ""
    var finalTid: Option[String] = None
    if (tid == None) {
      chosen = getTenantId
      finalTid = Some(chosen)
    } else {
      finalTid = tid
    }


    //val gitMsgFile = "https://raw.githubusercontent.com/ligadata-dhaval/Kamanja/master/HelloWorld_Msg_Def.json"
    if (input == "") {
      containerFileDir = metadataAPI.GetMetadataAPIConfig.getProperty("CONTAINER_FILES_DIR")
      if (containerFileDir == null) {
        response = "CONTAINER_FILES_DIR property missing in the metadata API configuration"
      } else {
        //verify the directory where messages can be present
        IsValidDir(containerFileDir) match {
          case true => {
            //get all files with json extension
            val containers: Array[File] = new java.io.File(containerFileDir).listFiles.filter(_.getName.endsWith(".json"))
            containers.length match {
              case 0 => {
                response="Container not found at " + containerFileDir
              }
              case option => {
                val containerDefs = getUserInputFromMainMenu(containers)
                for (containerDef <- containerDefs) {
                  response += metadataAPI.AddContainer(containerDef.toString, "JSON", userid, finalTid)
                }
              }
            }
          }
          case false => {
            //println("Message directory is invalid.")
            response = "Message directory is invalid."
          }
        }
      }
    } else {
      //input provided
      var container = new File(input.toString)
      if( container.exists()){
        val containerDef = Source.fromFile(container).mkString
        response = metadataAPI.AddContainer(containerDef, "JSON", userid, finalTid)
      }else{
        response = "Input container file does not exist"
      }
    }
    //Got the container.
    response
  }

  def updateContainer(input: String, tid: Option[String] = None): String ={
    var response = ""
    var containerFileDir: String = ""

    //val gitMsgFile = "https://raw.githubusercontent.com/ligadata-dhaval/Kamanja/master/HelloWorld_Msg_Def.json"
    var chosen: String = ""
    var finalTid: Option[String] = None
    if (tid == None) {
      chosen = getTenantId
      finalTid = Some(chosen)
    } else {
      finalTid = tid
    }


    //val gitMsgFile = "https://raw.githubusercontent.com/ligadata-dhaval/Kamanja/master/HelloWorld_Msg_Def.json"
    if (input == "") {
      containerFileDir = metadataAPI.GetMetadataAPIConfig.getProperty("CONTAINER_FILES_DIR")
      if (containerFileDir == null) {
        response = "CONTAINER_FILES_DIR property missing in the metadata API configuration"
      } else {
        //verify the directory where messages can be present
        IsValidDir(containerFileDir) match {
          case true => {
            //get all files with json extension
            val containers: Array[File] = new java.io.File(containerFileDir).listFiles.filter(_.getName.endsWith(".json"))
            containers.length match {
              case 0 => {
                response="Container not found at " + containerFileDir
              }
              case option => {
                val containerDefs = getUserInputFromMainMenu(containers)
                for (containerDef <- containerDefs) {
                  response += metadataAPI.UpdateContainer(containerDef.toString, "JSON", userid, finalTid)
                }
              }
            }
          }
          case false => {
            //println("Message directory is invalid.")
            response = "Message directory is invalid."
          }
        }
      }
    } else {
      //input provided
      var container = new File(input.toString)
      val containerDef = Source.fromFile(container).mkString
      // 1118 Changes begin - Changed AddContiner to Update Container to follow the path correctly
      response = metadataAPI.UpdateContainer(containerDef, "JSON", userid,  finalTid)
      // 1118 Changes end
    }
    //Got the container.
    response
  }

  def getContainer(param: String = "", tid : Option[String] = None): String ={
    var response=""
    if (param.length > 0) {
      val(ns, name, ver) = com.ligadata.kamanja.metadata.Utils.parseNameToken(param)
      try {
        return metadataAPI.GetContainerDefFromCache(ns, name,"JSON", ver, userid, tid)
      } catch {
        case e: Exception => logger.error("", e)
      }
    }
    val containerKeys = metadataAPI.GetAllContainersFromCache(true, None, tid)

    if (containerKeys.length == 0) {
      response="Sorry, No containers available in the Metadata"
    }else{
      println("\nPick the container from the list: ")
      var srNo = 0
      for(containerKey <- containerKeys){
        srNo+=1
        println("["+srNo+"] "+containerKey)
      }
      print("\nEnter your choice: ")
      val choice: Int = readInt()

      if (choice < 1 || choice > containerKeys.length) {
        response="Invalid choice " + choice + ",start with main menu..."
      }else{
        val containerKey = containerKeys(choice - 1)
        /*val contKeyTokens = containerKey.split("\\.")
        val contNameSpace = contKeyTokens(0)
        val contName = contKeyTokens(1)
        val contVersion = contKeyTokens(2)*/
        val(ns, name, ver) = com.ligadata.kamanja.metadata.Utils.parseNameToken(containerKey)
        response=metadataAPI.GetContainerDefFromCache(ns, name, "JSON", ver, userid, tid)
      }
    }
    response
  }

  def getAllContainers (tid : Option[String] = None) : String ={
    var response = ""
    var containerKeysList = ""
    try {
      // 646 - 672 Changes begin - filter based on tenantId
      val containerKeys: Array[String] = metadataAPI.GetAllContainersFromCache(true, userid, tid)
      // 646 - 672 Changes end

      if (containerKeys.length == 0) {
        var emptyAlert = "Sorry, No containers are available in the Metadata"
        response=(new ApiResult(ErrorCodeConstants.Success, "ContainerService",null, emptyAlert)).toString
      } else {

        response= (new ApiResult(ErrorCodeConstants.Success, "ContainerService", containerKeys.mkString(", "), "Successfully retrieved all the messages")).toString

      }
    } catch {
      case e: Exception => {
        logger.warn("", e)
        response = e.getStackTrace.toString
        response= (new ApiResult(ErrorCodeConstants.Failure, "ContainerService",null, response)).toString
      }
    }
    response
  }

  def removeContainer(parm: String = ""): String ={
    var response = ""
    try{

       if (parm.length > 0) {
         val(ns, name, ver) = com.ligadata.kamanja.metadata.Utils.parseNameToken(parm)
         try {
           return metadataAPI.RemoveContainer(ns, name, ver.toInt, userid)
         } catch {
           case e: Exception => logger.error("", e)
         }
      }

      val contKeys = metadataAPI.GetAllContainersFromCache(true, None)

      if (contKeys.length == 0) {
        response=("Sorry, No containers available in the Metadata")
      }else{
        println("\nPick the container to be deleted from the following list: ")
        var seq = 0
        contKeys.foreach(key => { seq += 1; println("[" + seq + "] " + key) })

        print("\nEnter your choice: ")
        val choice: Int = readInt()

        if (choice < 1 || choice > contKeys.length) {
          return ("Invalid choice " + choice + ",start with main menu...")
        }else{
          val contKey = contKeys(choice - 1)
          val(contNameSpace, contName, contVersion) = com.ligadata.kamanja.metadata.Utils.parseNameToken(contKey)
          return metadataAPI.RemoveContainer(contNameSpace, contName, contVersion.toLong, userid)
        }
      }
    } catch {
      case e: NumberFormatException => {
        response=("\n Entry not in desired format. Please enter only one choice correctly")
      }
      case e: Exception => {
        logger.warn("", e)
        response=(e.toString)
      }
    }
    response
  }

  //utilities
  def IsValidDir(dirName: String): Boolean = {
    val iFile = new File(dirName)
    if (!iFile.exists) {
      println("The File Path (" + dirName + ") is not found: ")
      false
    } else if (!iFile.isDirectory) {
      println("The File Path (" + dirName + ") is not a directory: ")
      false
    } else
      true
  }

  private def getTenantId: String = {
    var tenatns = metadataAPI.GetAllTenants(userid)
    return getUserInputFromMainMenu(tenatns)
  }

  def getUserInputFromMainMenu(tenants: Array[String]) : String = {
    var srNo = 0
    for(tenant <- tenants) {
      srNo += 1
      println("[" + srNo + "]" + tenant)
    }
    print("\nEnter your choice(If more than 1 choice, please use commas to seperate them): \n")
    val userOption: Int = readLine().trim.toInt
    return tenants(userOption - 1)
  }

  def getUserInputFromMainMenu(containers: Array[File]): Array[String] = {
    var listOfContainerDef: Array[String] = Array[String]()
    var srNo = 0
    println("\nPick a Container Definition file(s) from below choices\n")
    for (container <- containers) {
      srNo += 1
      println("[" + srNo + "]" + container)
    }
    print("\nEnter your choice(If more than 1 choice, please use commas to seperate them): \n")
    val userOptions: List[Int] = readLine().filter(_ != '\n').split(',').filter(ch => (ch != null && ch != "")).map(_.trim.toInt).toList
    //check if user input valid. If not exit
    for (userOption <- userOptions) {
      userOption match {
        case userOption if (1 to srNo).contains(userOption) => {
          //find the file location corresponding to the message
          var container = containers(userOption - 1)
          //process message
          val containerDef = Source.fromFile(container).mkString
          listOfContainerDef = listOfContainerDef :+ containerDef
        }
        case _ => {
          println("Unknown option: ")
        }
      }
    }
    listOfContainerDef
  }
}
