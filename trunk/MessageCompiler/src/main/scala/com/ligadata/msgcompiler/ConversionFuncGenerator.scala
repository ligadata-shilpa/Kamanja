package com.ligadata.msgcompiler

import com.ligadata.Exceptions._;
import com.ligadata.Exceptions.StackTrace;
import org.apache.logging.log4j.{ Logger, LogManager }
import com.ligadata.kamanja.metadata._;
import scala.collection.mutable.ArrayBuffer

class ConversionFuncGenerator {
  val logger = this.getClass.getName
  lazy val log = LogManager.getLogger(logger)
  val messageStr = "message";
  val containerStr = "container";
  var msgConstants = new MessageConstants

  /*
   * Get Previous version msg
   */
  def getPrevVersionMsg(message: Message, mdMgr: MdMgr): String = {
    var prevVerMsgObjstr: String = ""
    log.info("Case Stmts****************************** ");

    var prevVerMsgBaseTypesIdxArry = new ArrayBuffer[String]
    val Fixed = msgConstants.isFixedFunc(message);
    val isMsg = msgConstants.isMessageFunc(message);
    val msgdefArray = getPrevVersionMsgContainers(message, mdMgr);
    if (msgdefArray == null) return null;

    msgdefArray.foreach(msgdef => {
      //call the function which generates the complete conversion function and also another string with case stmt and append to string buffer 
      if (msgdef != null) {
        val (caseStmt, convertFunc) = getconversionFunc(msgdef, isMsg, Fixed)
      }
    })
    return " "
  }
  /*
   * Get Conversion Func for each prev version 
   */
  private def getconversionFunc(msgdef: ContainerDef, isMsg: Boolean, fixed: Boolean): (String, String) = {
    var attributes: Map[String, Any] = Map[String, Any]()
    var caseStmt: String = "";
    var conversionFunc: String = "";
    try {
      if (msgdef != null) {
        val childAttrs = getPrevVerMsgAttributes(msgdef, isMsg, fixed)
        attributes = childAttrs
        attributes.foreach(a => println(a._1 + "========" + a._2.asInstanceOf[AttributeDef].aType.implementationName))
        caseStmt = generateCaseStmts(msgdef)
        log.info("Case Stmts******************************  : " + caseStmt);

        /* if ((msgdef.dependencyJarNames != null) && (msgdef.JarName != null))
          message.Jarset = message.Jarset + pMsgdef.JarName ++ pMsgdef.dependencyJarNames
        else if (pMsgdef.JarName != null)
          message.Jarset = message.Jarset + pMsgdef.JarName
        else if (pMsgdef.dependencyJarNames != null)
          message.Jarset = message.Jarset ++ pMsgdef.dependencyJarNames*/
      }
    } catch {
      case e: Exception => {
        log.debug("", e)
      }
    }
    (caseStmt, conversionFunc)
  }

  /*
   * Get the previous version messages from metadata if exists 
   */
  private def getPrevVersionMsgContainers(message: Message, mdMgr: MdMgr): Array[ContainerDef] = {
    var msgdefArray = new scala.collection.mutable.ArrayBuffer[ContainerDef]
    var cntrdefobjs: Option[scala.collection.immutable.Set[ContainerDef]] = null
    var msgdefobjs: Option[scala.collection.immutable.Set[_]] = null

    var prevVerMsgObjstr: String = ""
    var childs: ArrayBuffer[(String, String)] = ArrayBuffer[(String, String)]()
    var isMsg: Boolean = false
    try {
      val messagetype = message.MsgType
      val namespace = message.NameSpace
      val name = message.Name
      val ver = message.Version
      var ctr: Option[ContainerDef] = null;

      if (namespace == null || namespace.trim() == "")
        throw new Exception("Proper Namespace do not exists in message/container definition")
      if (name == null || name.trim() == "")
        throw new Exception("Proper Name do not exists in message")
      if (ver == null || ver.trim() == "")
        throw new Exception("Proper Version do not exists in message/container definition")

      if (messagetype != null && messagetype.trim() != "") {
        log.info("Case Stmts 1 ****************************** ");

        if (messagetype.equalsIgnoreCase(messageStr)) {
          msgdefobjs = mdMgr.Messages(namespace, name, false, false)
          log.info("Case Stmts 2 ****************************** ");

          val isMsg = true
        } else if (messagetype.equalsIgnoreCase(containerStr)) {
          msgdefobjs = mdMgr.Containers(namespace, name, false, false)
        }

        if (msgdefobjs != null) {
          msgdefobjs match {
            case None => {
              log.info("Case Stmts 5 ****************************** ");

              return null
            }
            case Some(m) =>
              {
                log.info("Case Stmts 6 ****************************** ");

                if (isMsg)
                  m.foreach(msgdef => msgdefArray += msgdef.asInstanceOf[MessageDef])
                else
                  m.foreach(msgdef => msgdefArray += msgdef.asInstanceOf[ContainerDef]) // val fullname = msgdef.FullNameWithVer.replaceAll("[.]", "_")           // prevVerMsgObjstr = msgdef.PhysicalName
              }
              return msgdefArray.toArray
          }
        }
      }
    } catch {
      case e: Exception => {
        log.debug("", e)
      }
    }
    return null;

  }

  /*
   * Get attributes from previous message
   */
  private def getPrevVerMsgAttributes(pMsgdef: ContainerDef, isMsg: Boolean, fixed: Boolean): (Map[String, Any]) = {
    var prevVerCtrdef: ContainerDef = new ContainerDef()
    var prevVerMsgdef: MessageDef = new MessageDef()
    var attributes: Map[String, Any] = Map[String, Any]()

    if (pMsgdef != null) {
      if (isMsg) {
        prevVerCtrdef = pMsgdef.asInstanceOf[MessageDef]
      } else {
        prevVerCtrdef = pMsgdef.asInstanceOf[ContainerDef]
      }
      if (fixed) {
        val memberDefs = prevVerCtrdef.containerType.asInstanceOf[StructTypeDef].memberDefs
        if (memberDefs != null) {
          attributes ++= memberDefs.filter(a => (a.isInstanceOf[AttributeDef])).map(a => (a.Name, a))
        }
      } else {
        val attrMap = prevVerCtrdef.containerType.asInstanceOf[MappedMsgTypeDef].attrMap
        if (attrMap != null) {
          attributes ++= attrMap.filter(a => (a._2.isInstanceOf[AttributeDef])).map(a => (a._2.Name, a._2))
        }
      }
    }
    (attributes)
  }

  /*
   * Generating Conversion Function for all fields
   */
  private def ConversionFunc(message: Message, prevMsgDef: ContainerDef) = {
    val msgName = message.Pkg + "." + message.Name

    """
    def convertPrevVersionToCurVersion(oldVerobj: Any): """ + msgName + """ = {
      try {
        oldVerobj match {
          """ + generateCaseStmts(prevMsgDef) + """
          case _ => {
            throw new Exception("Unhandled Version Found");
          }
        }
      } catch {
        case e: Exception => {
          log.debug("", e)
          throw e
        }
      }
      return null;
    }
  """
  }

  /*
   * Generate the case Stmts for prevobjects conversion
   */
  private def generateCaseStmts(msgdef: ContainerDef): String = {
    """
          case oldVerobj: """ + msgdef.PhysicalName + """ => { //matches current version
            return oldVerobj;
     }
    """
  }

  /*
   * generate FromFunc code for message fields 
   */
  private def getConversionFuncStr(message: Message, mdMgr: MdMgr): String = {
    var conversionFuncBuf = new StringBuilder(8 * 1024)
    try {
      if (message.Elements != null) {
        message.Elements.foreach(field => {
          if (field != null) {
            val fieldBaseType: BaseTypeDef = field.FldMetaataType
            val fieldType = fieldBaseType.tType.toString().toLowerCase()
            val fieldTypeType = fieldBaseType.tTypeType.toString().toLowerCase()
            fieldTypeType match {
              case "tscalar" => {
                // do nothing already added 
              }
              case "tcontainer" => {
                fieldType match {
                  case "tarray" => {
                    var arrayType: ArrayTypeDef = null
                    arrayType = fieldBaseType.asInstanceOf[ArrayTypeDef]
                    conversionFuncBuf = conversionFuncBuf.append(ConversionFuncForArray(field, true))
                  }
                  case "tarraybuf" => {
                    var arraybufType: ArrayBufTypeDef = null
                    arraybufType = fieldBaseType.asInstanceOf[ArrayBufTypeDef]
                    conversionFuncBuf = conversionFuncBuf.append(ConversionFuncForArray(field, false)) //fromFuncForArrayBufMapped(field))
                  }
                  case "tstruct" => {
                    var ctrDef: ContainerDef = mdMgr.Container(field.Ttype, -1, true).getOrElse(null) //field.FieldtypeVer is -1 for now, need to put proper version
                    conversionFuncBuf = conversionFuncBuf.append(ConversionFuncForStruct(field, ctrDef))
                  }
                  case "tmap" => {
                    conversionFuncBuf = conversionFuncBuf.append(ConversionFuncForMap(field))
                  }
                  case "tmsgmap" => {
                    var ctrDef: ContainerDef = mdMgr.Container(field.Ttype, -1, true).getOrElse(null) //field.FieldtypeVer is -1 for now, need to put proper version
                    conversionFuncBuf = conversionFuncBuf.append(ConversionFuncForStruct(field, ctrDef))
                  }
                  case _ => {
                    throw new Exception("This types is not handled at this time ") // BUGBUG - Need to handled other cases
                  }
                }
              }
              case _ => {
                throw new Exception("This types is not handled at this time ") // BUGBUG - Need to handled other cases
              }
            }
          }
        })
      }
    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    }
    return conversionFuncBuf.toString();
  }

  /*
   * Handlie Array
   */
  private def ConversionFuncForArray(field: Element, isArray: Boolean): String = {
    return null
  }

  private def ConversionFuncForStruct(field: Element, ctrDef: ContainerDef): String = {

    return null
  }

  private def ConversionFuncForMap(field: Element): String = {
    return null
  }

  private def ConversionFuncForScalar(field: Element, attributes: Map[String, Any], fixed: Boolean): String = {
    var prevObjDeserializedBuf = new StringBuilder(8 * 1024)
    var convertOldObjtoNewObjBuf = new StringBuilder(8 * 1024)
    var mappedPrevVerMatchkeys = new StringBuilder(8 * 1024)
    var mappedPrevTypNotrMatchkeys = new StringBuilder(8 * 1024)
    var prevObjTypNotMatchDeserializedBuf = new StringBuilder(8 * 1024)

    try {
      val typ = field.FldMetaataType;
      if (typ == null)
        throw new Exception("Type not found in metadata for Name: " + field.Name + " , NameSpace: " + field.NameSpace + " , Type : " + field.Ttype)
      if (field.Name == null || field.Name.trim() == "")
        throw new Exception("Field name do not exists")
      if (typ.FullName == null || typ.FullName.trim() == "")
        throw new Exception("Full name of Type " + field.Ttype + " do not exists in metadata ")

      var memberExists: Boolean = false
      var membrMatchTypeNotMatch = false // for mapped messages to handle if prev ver obj and current version obj member types do not match...
      var childTypeImplName: String = ""
      var childtypeName: String = ""
      var childtypePhysicalName: String = ""

      var sameType: Boolean = false
      if (attributes != null) {
        if (attributes.contains(field.Name)) {
          var child = attributes.getOrElse(field.Name, null)
          if (child != null) {
            val typefullname = child.asInstanceOf[AttributeDef].aType.FullName
            childtypeName = child.asInstanceOf[AttributeDef].aType.tTypeType.toString
            childtypePhysicalName = child.asInstanceOf[AttributeDef].aType.physicalName
            if (typefullname != null && typefullname.trim() != "" && typefullname.equals(typ.FullName)) {
              memberExists = true
            } else {
              membrMatchTypeNotMatch = true
              childTypeImplName = child.asInstanceOf[AttributeDef].aType.implementationName
            }
          }
        }
      }
      /*if (memberExists) {
        if (fixed) {

          prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s%s = prevVerObj.%s;%s".format(msgConstants.pad1, field.Name, field.Name, msgConstants.newline))
          convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append("%s%s = oldObj.%s;%s".format(msgConstants.pad2, field.Name, field.Name, msgConstants.newline))
        } else if (!fixed) {
          mappedPrevVerMatchkeys.append("\"" + field.Name + "\",")
          //if (baseTypIdx != -1)
          //prevObjDeserializedBuf = prevObjDeserializedBuf.append("%s case %s => fields(key) = (typIdx, prevObjfield._2._2);)%s".format(pad2, baseTypIdx, newline))

          //prevObjDeserializedBuf = prevObjDeserializedBuf.append("%sset(\"%s\", prevVerObj.getOrElse(\"%s\", null))%s".format(pad1, f.Name, f.Name, newline))
          // convertOldObjtoNewObjBuf = convertOldObjtoNewObjBuf.append("%sset(\"%s\", oldObj.getOrElse(\"%s\", null))%s".format(pad2, f.Name, f.Name, newline))
        }
      }
      if (membrMatchTypeNotMatch) {
        if (!fixed) {

          if (childtypeName.toLowerCase().equals("tscalar")) {

            val implName = typ.get.implementationName + ".Input"

            //  => data = com.ligadata.BaseTypes.StringImpl.toString(prevObjfield._2._2.asInstanceOf[String];
            mappedPrevTypNotrMatchkeys = mappedPrevTypNotrMatchkeys.append("\"" + f.Name + "\",")
            if (!prevVerMsgBaseTypesIdxArry.contains(childTypeImplName)) {
              prevVerMsgBaseTypesIdxArry += childTypeImplName
              prevObjTypNotMatchDeserializedBuf = prevObjTypNotMatchDeserializedBuf.append("%s case \"%s\" => data = %s.toString(value.asInstanceOf[%s]); %s".format(pad2, childTypeImplName, childTypeImplName, childtypePhysicalName, newline))
            }
          }
        }
      }*/

    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    }
    return null
  }

}