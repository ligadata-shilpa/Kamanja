package com.ligadata.msgcompiler

import com.ligadata.kamanja.metadata.MdMgr;

class GenerateRdd {

  var msgConstants = new MessageConstants

  def generateRdd(message: Message): (String, String) = {
    generateRddClass(message)
  }

  private def generateRddClass(message: Message): (String, String) = {

    var versionedRddClass: StringBuffer = new StringBuffer(1024)
    var nonVersionedRddClass: StringBuffer = new StringBuffer(1024)
    versionedRddClass.append(versionedPackagestmt(message) + msgConstants.newline + importstmts + msgConstants.newline + rddClass(message))
    nonVersionedRddClass.append(nonVersionedPackagestmt(message) + msgConstants.newline + importstmts + msgConstants.newline + rddClass(message))
    (versionedRddClass.toString(), nonVersionedRddClass.toString())
  }

  private def versionedPackagestmt(message: Message): String = {
    if (message.Pkg != null || message.Pkg.trim() != "") {
      val pkg = message.Pkg + ".V" +  MdMgr.ConvertVersionToLong(message.Version).toString 
      return msgConstants.packageStr.format(pkg, msgConstants.newline)
    } else return ""
  }

  private def nonVersionedPackagestmt(message: Message): String = {
    if (message.Pkg != null || message.Pkg.trim() != "") return msgConstants.packageStr.format(message.Pkg, msgConstants.newline)
    else return " "
  }

  private def importstmts() = {
    msgConstants.rddObjImportStmts
  }

  private def rddClass(message: Message) = {
    var rddClass: StringBuffer = new StringBuffer(1024)

    if (message.MsgType.equalsIgnoreCase("container")) {
      rddClass.append(msgConstants.rddFactoryClass.format(message.Name, msgConstants.newline) + msgConstants.pad1 + msgConstants.rddObj.format(message.Name, message.Name, msgConstants.newline) + msgConstants.pad1 + msgConstants.rddBaseContainerObj.format(message.Name, msgConstants.newline) + msgConstants.newline + msgConstants.closeBrace)
    } else if (message.MsgType.equalsIgnoreCase("message")) {
      rddClass.append(msgConstants.rddFactoryClass.format(message.Name, msgConstants.newline) + msgConstants.pad1 + msgConstants.rddObj.format(message.Name, message.Name, msgConstants.newline) + msgConstants.pad1 + msgConstants.newline + msgConstants.rddBaseMsgObj.format(message.Name, msgConstants.newline) + msgConstants.newline + msgConstants.closeBrace)
    }
    rddClass.toString()

  }

}