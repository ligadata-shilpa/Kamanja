package com.ligadata.msgcompiler

import org.apache.logging.log4j.{ Logger, LogManager }
import com.ligadata.kamanja.metadata._;
import com.ligadata.Exceptions._;

class MessageFieldTypesHandler {
  val logger = this.getClass.getName
  lazy val log = LogManager.getLogger(logger)
  val tInt: String = "tInt"
  val tChar: String = "tChar"
  val tString: String = "tString"
  val tLong: String = "tLong"
  val tFloat: String = "tFloat"
  val tDouble: String = "tDouble"
  val tBoolean: String = "tBoolean"
  val tArray: String = "tArray"
  val tContainer: String = "tContainer"
  val tScalar: String = "tScalar"

  //Handling the field types - get the metadata type for each field 
  def handleFieldTypes(message: Message, mdMgr: MdMgr): Message = {
    var argsList: List[(String, String, String, String, Boolean, String)] = List[(String, String, String, String, Boolean, String)]()
    var jarset: Set[String] = Set[String]();
    message.Elements.foreach(field => {
      //   log.info("fields name " + field.Name)
      //  log.info("fields type " + field.Ttype)
      val typ = MdMgr.GetMdMgr.Type(field.Ttype, -1, true) // message.Version.toLong

      if (typ.getOrElse("None").equals("None"))
        throw new Exception("Type not found in metadata for Name: " + field.Name + " , NameSpace: " + field.NameSpace + " , Version: " + message.Version + " , Type : " + field.Ttype)

      // to do - check null pointer if typ is not avaialble.....
      field.FldMetaataType = typ.get

      /*   log.info("******************TYPES FROM METADATA START******************************")
      log.info("type " + typ.get.tType.toString())
      log.info("******************TYPES FROM METADATA START******************************")
     
      */
    })

    message.Elements.foreach(field => {
      val types = getMetadataTypesForMsgFields(field, mdMgr)

      //get the fields args list for addding message in the metadata 
      argsList = (field.NameSpace, field.Name, field.FldMetaataType.NameSpace, field.FldMetaataType.Name, false, null) :: argsList

      //get the fields jarset for adding msg in the metadata
      jarset = jarset ++ getDependencyJarSet(field.FldMetaataType)

      field.FieldTypePhysicalName = types(0)

      /*
       log.info("****************** TYPES FROM METADATA START  --- In Message******************************")
       
      log.info("=========mesage fld type " + field.Ttype)
      log.info("=========mesage fld metadata type " + field.FldMetaataType.tType.toString())
      log.info("=========mesage fld metadata tTypeType " + field.FldMetaataType.tTypeType.toString())
      log.info("=========mesage fld metadata implementationName " + field.FldMetaataType.implementationName)
      log.info("=========mesage fld size " + types.size)
      log.info("=========mesage fld 2 :  " + types(1))
      log.info("=========mesage fld  " + field.FieldTypePhysicalName)
      log.info("******************TYPES FROM METADATA End --- In Message ******************************")
      * */

    })

    // set the field args list and jarset in message object to retrieve while adding mesage to metadata
    message.ArgsList = argsList
    message.Jarset = jarset

    return message
  }

  private def getDependencyJarSet(fieldBaseType: BaseTypeDef): Set[String] = {

    var jarset: Set[String] = Set[String]();

    if ((fieldBaseType.dependencyJarNames != null) && (fieldBaseType.JarName != null))
      jarset = jarset + fieldBaseType.JarName ++ fieldBaseType.dependencyJarNames
    else if (fieldBaseType.JarName != null)
      jarset = jarset + fieldBaseType.JarName
    else if (fieldBaseType.dependencyJarNames != null)
      jarset = jarset ++ fieldBaseType.dependencyJarNames

    return jarset

  }

  private def getMetadataTypesForMsgFields(field: Element, mdMgr: MdMgr): Array[String] = {

    val fieldBaseType: BaseTypeDef = field.FldMetaataType
    var arrayType: ArrayTypeDef = null
    var types: Array[String] = new Array[String](2);
    val fieldType = fieldBaseType.tType.toString()
    val fieldTypeType = fieldBaseType.tTypeType.toString()
    if (fieldBaseType.isInstanceOf[ArrayTypeDef])
      arrayType = fieldBaseType.asInstanceOf[ArrayTypeDef]

    // log.info("fieldTypeType " + fieldTypeType)

    fieldTypeType match {
      case "tScalar" => {
        types(0) = fieldBaseType.PhysicalName
        types(1) = ""

      }
      case "tArray" => {
        types(0) = arrayType.typeString
        types(1) = ""
      }
      case "tContainer" => {
        var ctrDef: ContainerDef = mdMgr.Container(field.Ttype, -1, true).getOrElse(null) //field.FieldtypeVer is -1 for now, need to put proper version
        types(0) = ctrDef.PhysicalName
        types(1) = ""
      }
      case _ => {
        throw new Exception("This types is not handled at this time ") // BUGBUG - Need to handled other cases
      }

    }
    return types
  }

  /*
   * Get the arguments list of all fields for the message
   */

}