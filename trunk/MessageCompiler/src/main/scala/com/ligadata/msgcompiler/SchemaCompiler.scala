package com.ligadata.msgcompiler

import org.apache.logging.log4j.{ Logger, LogManager }
import com.ligadata.kamanja.metadata._;

class SchemaCompiler {
  val recordType = "\\\"type\\\": \\\"record\\\", ";
  val fieldsConstant = "\\\"fields\\\":";
  val avroType = "type";
  val bslash = "\\";
  val quote = "\"";
  val colon = ":";
  val comma = ",";
  val openSqBrace = "[";
  val closeSqBrace = "]";
  val openBrace = "{";
  val closeBrace = "}";
  val integer = "integer";
  val primitiveTypes = Array("null", "int", "long", "float", "double", "bytes", "string");
  val int = "int";

  val logger = this.getClass.getName
  lazy val log = LogManager.getLogger(logger)
  /*
   * Generate the AVRO Schema
   */
  def generateAvroSchema(message: Message, mdMgr: MdMgr): Message = {
    var strBuf = new StringBuilder(8 * 1024);
    try {
      val fldsStr = getAvroFldTypes(message, mdMgr: MdMgr);

      strBuf = strBuf.append(openBrace + recordType)
      strBuf = strBuf.append(generateNameSpace(message.NameSpace) + comma)
      strBuf = strBuf.append(generateName(message.Name) + comma)
      strBuf = strBuf.append(fieldsConstant + openSqBrace)
      println("====" + fldsStr.substring(0, fldsStr.length() - 1));
      strBuf = strBuf.append(fldsStr)
      strBuf = strBuf.append(closeSqBrace)
      strBuf = strBuf.append(closeBrace)
      message.Schema = strBuf.toString();
    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    }

    return message
  }

  private def generateNameSpace(nameSpace: String): String = {

    return "\\\"namespace\\\" : \\\"" + nameSpace + "\\\"";
  }

  private def generateName(name: String): String = {

    return "\\\"name\\\" : \\\"" + name + "\\\"";
  }

  private def generateFieldType(fldType: String, slash: String, quote: String): String = {

    return "\\\"type\\\" : " + slash + quote + fldType + slash + quote;
  }

  private def generateRecordType: String = {
    return recordType;
  }

  private def genFields: String = {
    return fieldsConstant;
  }

  private def getAvroFldTypes(message: Message, mdMgr: MdMgr): String = {
    var strBuf = new StringBuilder(8 * 1024);
    var retFldStr: String = "";

    var arraySize: Int = message.Elements.size
    var avroFldsArray = new Array[String](arraySize);
    try {
      message.Elements.foreach(field => {
        var fldStr: String = "";
        val fieldTypestr = field.Ttype.split("\\.");

        if (fieldTypestr.size == 1) {
          if (primitiveTypes.contains(field.Ttype)) {
            fldStr = primitiveTypes(primitiveTypes.indexOf(field.Ttype));
          } else if (field.Ttype.equalsIgnoreCase(integer)) {
            fldStr = int;
          }
          retFldStr = generateFieldType(fldStr, bslash, quote)
        } else if (fieldTypestr.size == 2) {
          if (fieldTypestr(0).equalsIgnoreCase("system")) {
            retFldStr = parseFldStr(fieldTypestr)
          } else {

            //BUGBUG - handle if the container namespace is one word

          }
        } else if (fieldTypestr.size > 2) {
          /*
           * parse the namespace
           * get the name - see if it container or array of containers or map of containers
           * get the type from metadata and get object definition
           */
          val parseCtr = parseContainer(field, mdMgr)
          log.info("Parse Container from Object Definition" + parseCtr);
        }

        strBuf.append(openBrace + generateName(field.Name) + comma)
        strBuf.append(retFldStr + closeBrace + comma)

        avroFldsArray(field.FieldOrdinal) = fldStr;
      })
      val retStr = strBuf.toString().substring(0, strBuf.toString().length - 1)
      return retStr;

    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    }
  }

  /*
   * 
   */
  private def parseFldStr(fieldTypestr: Array[String]): String = {
    var fldStr: String = "";
    var retFldStr: String = "";
    try {

      val fieldTypestr1 = fieldTypestr(1);
      if (primitiveTypes.contains(fieldTypestr(1))) {

        fldStr = primitiveTypes(primitiveTypes.indexOf(fieldTypestr1));
        retFldStr = generateFieldType(fldStr, bslash, quote)
      } else if (fieldTypestr(1).equalsIgnoreCase(integer)) {

        fldStr = int;
        retFldStr = generateFieldType(fldStr, bslash, quote)

      } else if (fieldTypestr1.startsWith("array")) {

        fldStr = parseArrayPrimitives(fieldTypestr1)
        retFldStr = generateFieldType(fldStr, "", "")
        // println("retFldStr   " + retFldStr);
      } else if (fieldTypestr(1).startsWith("map")) {

        fldStr = parseMapPrimitives(fieldTypestr1)
        retFldStr = generateFieldType(fldStr, "", "")
      }
    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    }
    retFldStr
  }
  /*
   * parse the arryofint type string
   */
  private def parseArrayPrimitives(arrayType: String): String = {
    var retFldtypeStr: String = "";
    var ftype: String = "";
    var fldtypeStr: String = "";
    if (arrayType.startsWith("array")) {
      fldtypeStr = arrayType.substring(7, arrayType.length);

      if (primitiveTypes.contains(fldtypeStr))
        ftype = primitiveTypes(primitiveTypes.indexOf(fldtypeStr));
    } else if (fldtypeStr.startsWith("array")) {
      //handle arrayofarrayofint
      ftype = parseArrayPrimitives(fldtypeStr)
    }

    retFldtypeStr = "{\\\"type\\\" : \\\"array\\\", \\\"items\\\" : \\\"" + ftype + "\\\"}";
    retFldtypeStr
  }

  /*
   * parse mapofStringFloat type
   */

  private def parseMapPrimitives(mapType: String): String = {
    var retFldtypeStr: String = "";
    var ftype: String = "";
    var fldtypeStr: String = "";
    if (mapType.startsWith("map")) {
      fldtypeStr = mapType.substring(5);

      if (fldtypeStr.startsWith("string")) {
        val flsValType = fldtypeStr.substring(6)
        log.info("flsValType  " + flsValType)
        if (primitiveTypes.contains(flsValType)) {
          ftype = primitiveTypes(primitiveTypes.indexOf(flsValType));
        } else if (fldtypeStr.startsWith("array")) {
          //handle array of arrayofint
          ftype = parseArrayPrimitives(flsValType)
        }
      }
    }

    retFldtypeStr = "{\\\"type\\\" : \\\"map\\\", \\\"values\\\" : \\\"" + ftype + "\\\"}"; //{"type" : "map", "values" : "int"}
    retFldtypeStr
  }

  /*
   * parse the message/container type for message fields 
   */
  private def parseContainer(field: Element, mdMgr: MdMgr): String = {
    var containerTypeStr = new StringBuilder(8 * 1024)
    var msgdefStr: String = "";
    var messageParser = new MessageParser
    var message: Message = null

    try {
      log.info("field.Name" + field.Name);
      val fieldBaseType: BaseTypeDef = field.FldMetaataType
      log.info("field.fieldBaseType" + fieldBaseType);

      val fieldType = fieldBaseType.tType.toString().toLowerCase()
      val fieldTypeType = fieldBaseType.tTypeType.toString().toLowerCase()
      fieldTypeType match {
        case "tscalar" => {
          //  fromFuncBuf = fromFuncBuf.append(fromFuncForScalarFixed(field))
          //log.info("fieldBaseType.implementationName    " + fieldBaseType.implementationName)
        }
        case "tcontainer" => {
          fieldType match {

            case "tarray" => {
              var arrayType: ArrayTypeDef = fieldBaseType.asInstanceOf[ArrayTypeDef]
              if (arrayType != null) {
                var ctrDef: ContainerDef = mdMgr.Container(arrayType.elemDef.FullName, -1, true).getOrElse(null) //field.FieldtypeVer is -1 for now, need to put proper version

                msgdefStr = ctrDef.objectDefinition
                log.info("3333*************************************************" + msgdefStr);
                message = messageParser.processJson(msgdefStr, mdMgr, false)
                message = generateAvroSchema(message, mdMgr)
                val typeStr = "\"type\": {\"type\": \"array\", \"items\": {"
                containerTypeStr.append(typeStr + message.Schema + closeBrace + closeBrace)
                log.info("*************************************************" + typeStr);

              }
              //  fromFuncBuf = fromFuncBuf.append(fromFuncForArrayFixed(field))
            }
            case "tarraybuf" => {
              var arraybufType: ArrayBufTypeDef = null
              arraybufType = fieldBaseType.asInstanceOf[ArrayBufTypeDef]
              if (arraybufType != null) {
                msgdefStr = arraybufType.elemDef.objectDefinition
                message = messageParser.processJson(msgdefStr, mdMgr, false)
                message = generateAvroSchema(message, mdMgr)
                val typeStr = "\"type\": {\"type\": \"array\", \"items\": {"
                containerTypeStr.append(typeStr + message.Schema + closeBrace + closeBrace)
                log.info("*************************************************" + typeStr);

              }
            }
            case "tstruct" => {
              var ctrDef: ContainerDef = mdMgr.Container(field.Ttype, -1, true).getOrElse(null) //field.FieldtypeVer is -1 for now, need to put proper version
              if (ctrDef != null) {
                msgdefStr = ctrDef.objectDefinition
                message = messageParser.processJson(msgdefStr, mdMgr, false)
                message = generateAvroSchema(message, mdMgr)

                containerTypeStr.append(message.Schema)
              }
            }
            case "tmsgmap" => {
              var ctrDef: ContainerDef = mdMgr.Container(field.Ttype, -1, true).getOrElse(null) //field.FieldtypeVer is -1 for now, need to put proper version
              if (ctrDef != null) {
                msgdefStr = ctrDef.objectDefinition
                message = messageParser.processJson(msgdefStr, mdMgr, false)
                message = generateAvroSchema(message, mdMgr)

                containerTypeStr.append("\"type\": " + message.Schema)
                log.info("*************************************************" + containerTypeStr.toString());

              }
              //    fromFuncBuf = fromFuncBuf.append(fromFuncForStructFixed(field))
            }
            case "tmap" => {
              var maptypeDef: MapTypeDef = null;
              maptypeDef = fieldBaseType.asInstanceOf[MapTypeDef]
             
              log.info(maptypeDef.valDef.tType)
            }
            case _ => {
              throw new Exception("This types is not handled at this time ") // BUGBUG - Need to handled other cases
            }
          }
        }
      }
    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    }
    return containerTypeStr.toString();
  }

}