package com.ligadata.msgcompiler

import org.apache.log4j.Logger;
import com.ligadata.kamanja.metadata._;
import com.ligadata.Exceptions._;

class CreateMessage {

  val logger = this.getClass.getName
  lazy val log = Logger.getLogger(logger)

  /*
   * create the message to add message in metadata (call MakeFixedMsg )
   */
  def createMessage(msg: Message, mdMgr: MdMgr, recompile: Boolean = false): ContainerDef = {

    createFixedMsgDef(msg, mdMgr, recompile)
  }

 
  /*
   * create the message
   */
  private def createFixedMsgDef(msg: Message, mdMgr: MdMgr, recompile: Boolean = false): MessageDef = {
    var msgDef: MessageDef = new MessageDef()

    try {
      var version = MdMgr.ConvertVersionToLong(msg.Version)
      if (msg.Persist) {
        if (msg.PartitionKey == null || msg.PartitionKey.size == 0) {
          throw new Exception("Please provide parition keys in the MessageDefinition since the Message will be Persisted based on Partition Keys")
        }
      }
      
      log.info("msg.ArgsList   "+msg.ArgsList.toList)
      log.info("msg.jarset    "+ msg.Jarset.toList)
     
      if (msg.PartitionKey != null)
        msgDef = mdMgr.MakeFixedMsg(msg.NameSpace, msg.Name, msg.PhysicalName, msg.ArgsList, version, null, msg.Jarset.toArray, null, null, msg.PartitionKey.toArray, recompile, msg.Persist)
      else
        msgDef = mdMgr.MakeFixedMsg(msg.NameSpace, msg.Name, msg.PhysicalName, msg.ArgsList, version, null, msg.Jarset.toArray, null, null, null, recompile, msg.Persist)
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        log.debug("StackTrace:" + stackTrace)
        throw e
      }
    }
    msgDef
  }
}