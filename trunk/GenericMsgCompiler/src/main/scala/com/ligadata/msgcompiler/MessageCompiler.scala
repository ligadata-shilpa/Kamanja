package com.ligadata.msgcompiler

import scala.util.parsing.json.JSON;
import scala.io.Source;

import java.io.File;
import java.io.PrintWriter;
import java.util.Date;

import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.ArrayBuffer;

import org.json4s.jackson.JsonMethods._;
import org.json4s.DefaultFormats;
import org.json4s.Formats;
import org.apache.log4j.Logger;

import com.ligadata.kamanja.metadata._;
import com.ligadata.Exceptions._;

class Messages(var messages: List[Message])
class Message(var MsgType: String, var NameSpace: String, var Name: String, var PhysicalName: String, var Version: String, var Description: String, var Fixed: String, var Persist: Boolean, var Elements: List[Element], var TDataExists: Boolean, var TrfrmData: TransformData, var Jarset: Set[String], var Pkg: String, var Ctype: String, var CCollectiontype: String, var Containers: List[String], var PartitionKey: List[String], var PrimaryKeys: List[String], var ClsNbr: Long, var MsgLvel: Int, var ArgsList: List[(String, String, String, String, Boolean, String)])
class TransformData(var input: Array[String], var output: Array[String], var keys: Array[String])
class Field(var NameSpace: String, var Name: String, var Ttype: String, var CollectionType: String, var Fieldtype: String, var FieldtypeVer: String)
class Element(var NameSpace: String, var Name: String, var Ttype: String, var CollectionType: String, var ElemType: String, var FieldtypeVer: String, var FieldOrdinal: Int, var FldMetaataType: BaseTypeDef, var FieldTypePhysicalName: String)
class MessageGenObj(var verScalaClassStr: String, var verJavaClassStr: String, var containerDef: ContainerDef, var noVerScalaClassStr: String, var noVerJavaClassStr: String, var argsList: List[(String, String, String, String, Boolean, String)])

object MessageCompiler {

  val logger = this.getClass.getName
  lazy val log = Logger.getLogger(logger)
  var msgGen: MessageGenerator = new MessageGenerator
  var handleMsgFieldTypes: MessageFieldTypesHandler = new MessageFieldTypesHandler
  var createMsg: CreateMessage = new CreateMessage

  /*
   * parse the message definition json,  add messages to metadata and create the Fixed and Mapped Mesages
   */
  def processMsgDef(jsonstr: String, msgDfType: String, mdMgr: MdMgr, recompile: Boolean = false): (String, ContainerDef) = {

    var messageParser = new MessageParser
    var messages: Messages = null
    var generatedMsg: String = ""
    var containerDef: ContainerDef = null

    try {
      if (mdMgr == null)
        throw new Exception("MdMgr is not found")
      if (msgDfType.equalsIgnoreCase("json")) {
        messages = messageParser.processJson(jsonstr, mdMgr, recompile)
        log.info("=============Started================" + messages.messages.size)
        messages.messages.foreach(msg => {
          handleMsgFieldTypes.handleFieldTypes(msg, mdMgr)
          generatedMsg = msgGen.generateMessage(msg)
          containerDef = createMsg.createMessage(msg, mdMgr, recompile)
          log.info("===================" + msg.MsgLvel)
          log.info("===================" + msg.Name)
          log.info("===================" + msg.NameSpace)
          log.info("===================" + msg.Elements)
          msg.Elements.foreach(e => {
            log.info("=================" + e.Name)
            log.info("=================" + e.Ttype)
          })
          log.info("============== Generated Message Start==============")
          log.info("Name =============== " + containerDef.Name)
          log.info("NameSpace =============== " + containerDef.NameSpace)
          log.info("typeString =============== " + containerDef.typeString)
          log.info("CheckAndGetDependencyJarNames =============== " + containerDef.CheckAndGetDependencyJarNames.toList)
          log.info("containerType.keys =============== " + containerDef.containerType.keys)
          log.info("FullNameWithVer =============== " + containerDef.FullNameWithVer)
          log.info(" JarName =============== " + containerDef.JarName)
          log.info("PhysicalName =============== " + containerDef.PhysicalName)
          log.info("TranId =============== " + containerDef.TranId)
         
        })

        //simple check to get metadata types for the fields in message

        // --- messages.messages.foreach(msg => { message = handleMsgFieldTypes.handleFieldTypes(message, mdMgr) })

        // end getting the metadata types for fields

        // ---- generatedMsg = msgGen.generateMessage(message)        

        // add message to metadata
        //--- messageDef  = createMsg.createMessage(message, mdMgr,  recompile)

        /*
           log.info("===================" + msg.NameSpace)
          log.info("===================" + msg.ArgsList)
          log.info("===================" + msg.Jarset)
          log.info("===================" + msg.MsgLvel)
          log.info("===================" + msg.MsgType)
          log.info("===================" + msg.Elements)
         
         */

      } else throw new Exception("MsgDef Type JSON is only supported")

    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        log.debug("StackTrace:" + stackTrace)
        throw e
      }
    }
    return (generatedMsg, containerDef)
  }

}