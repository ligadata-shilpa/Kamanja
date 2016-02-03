package com.ligadata.msgcompiler

import com.ligadata.Exceptions._;
import com.ligadata.Exceptions.StackTrace;
import org.apache.logging.log4j.{ Logger, LogManager }
import com.ligadata.kamanja.metadata._;

class MappedMsgGenerator {

  var builderGenerator = new MessageBuilderGenerator
  var msgObjectGenerator = new MessageObjectGenerator
  var msgConstants = new MessageConstants
  val logger = this.getClass.getName
  lazy val log = LogManager.getLogger(logger)

  def AddArraysInConstructor(fields: List[Element]): String = {
    AddArraysInConstrtor(fields)

  }

  private def AddArraysInConstrtor(fields: List[Element]): String = {
    var addArrays = new StringBuilder(8 * 1024)
    try {

      fields.foreach(field => {
        var addArraysStr: String = ""
        val fieldBaseType: BaseTypeDef = field.FldMetaataType
        val fieldType = fieldBaseType.tType.toString().toLowerCase()
        val fieldTypeType = fieldBaseType.tTypeType.toString().toLowerCase()
        fieldType match {
          case "tarray" => {
            addArraysStr = "\tfields(\"" + field.Name + "\") = (-1, new " + field.FieldTypePhysicalName + "(0));\n "
          }
          case "tarraybuf" => {
            var arraybufType: ArrayBufTypeDef = null
            arraybufType = fieldBaseType.asInstanceOf[ArrayBufTypeDef]
            addArraysStr = "\tfields(\"" + field.Name + "\") = (-1, new " + field.FieldTypePhysicalName + "(0));\n "
          }
          case _ => { addArraysStr = "" }
        }
        addArrays = addArrays.append(addArraysStr.toString())
      })
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        log.debug("StackTrace:" + stackTrace)
        throw e
      }
    }
    val addArrayConstrter: String = addArrays.toString
    if (addArrayConstrter == null || addArrayConstrter.trim == "")
      return ""
    else return """
    AddArraysInConstructor
    private def AddArraysInConstructor: Unit = {
      """ + addArrayConstrter + """
    }
    """
  }

  /**
   * Primary Keys variable declaration
   */

  def primayPartitionKeysVar(message: Message): String = {
    //log.info("primary Keys decl " + primayParitionKeysDecl(message))
    primayParitionKeysDecl(message)

  }

  /**
   * Primary Keys variable declaration
   */

  private def primayParitionKeysDecl(message: Message): String = {
    var primaryKeysStr: String = ""
    var primaryKeys = new StringBuilder(8 * 1024)
    var keysSet = Set[String]()

    if (message.PrimaryKeys == null || message.PrimaryKeys.size == 0) {
      primaryKeysStr = "";
      primaryKeys = primaryKeys.append(primaryKeysStr)
    } else {
      message.PrimaryKeys.foreach(key => {
        keysSet = keysSet + key
      })
      message.PartitionKeys.foreach(key => {
        keysSet = keysSet + key
      })
      keysSet.foreach(key => {
        message.Elements.foreach(field => {
          if (field.Name.equalsIgnoreCase(key))
            primaryKeysStr = "\t var " + field.Name + ":" + field.FieldTypePhysicalName + "= _;\n";
        })
        primaryKeys = primaryKeys.append(primaryKeysStr)
      })
    }
    return primaryKeys.toString()

  }

  /*
   * Set Method Generation Function for Mapped Messages
   */
  def setFuncGenerationforMapped(fields: List[Element], fldsMap: Map[String, Int], mdMgr: MdMgr): String = {
    setFuncGenforMapped(fields, fldsMap, mdMgr)
  }

  private def setFuncGenforMapped(fields: List[Element], fldsMap: Map[String, Int], mdMgr: MdMgr): String = {
    var setMethod = new StringBuilder(8 * 1024)

    try {
      fields.foreach(field => {
        var setmethodStr: String = ""
        val fieldBaseType: BaseTypeDef = field.FldMetaataType
        val fieldType = fieldBaseType.tType.toString().toLowerCase()
        val fieldTypeType = fieldBaseType.tTypeType.toString().toLowerCase()

        // log.info("fieldTypeType " + fieldTypeType)
        // log.info("fieldBaseType 1 " + fieldBaseType.tType)
        //  log.info("fieldBaseType 2 " + fieldBaseType.typeString)
        //  log.info("fieldBaseType 3" + fieldBaseType.tTypeType)

        //   log.info("fieldType " + fieldType)

        fieldTypeType match {

          case "tscalar" => {
            setmethodStr = setMethodForScalarMapped(field, fldsMap)
            //log.info("fieldBaseType.implementationName    " + fieldBaseType.implementationName)

          }
          case "tcontainer" => {
            fieldType match {
              case "tarray" => {
                var arrayType: ArrayTypeDef = null
                arrayType = fieldBaseType.asInstanceOf[ArrayTypeDef]
                setmethodStr = setMethodForStructMapped(field)
              }
              case "tarraybuf" => {
                var arraybufType: ArrayBufTypeDef = null
                arraybufType = fieldBaseType.asInstanceOf[ArrayBufTypeDef]
                setmethodStr = setMethodForStructMapped(field)

              }
              case "tstruct" => {
                var ctrDef: ContainerDef = mdMgr.Container(field.Ttype, -1, true).getOrElse(null) //field.FieldtypeVer is -1 for now, need to put proper version
                setmethodStr = setMethodForStructMapped(field)
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

        setMethod = setMethod.append(setmethodStr.toString())
        // log.info("=========SET Methods ===============" + setMethod.toString());
      })
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        log.debug("StackTrace:" + stackTrace)
        throw e
      }
    }
    return setMethod.toString
  }

  /**
   * Set method for mapped messages
   */
  private def setMethodForScalarMapped(field: Element, fldsMap: Map[String, Int]): String = {

    val fieldBaseType: BaseTypeDef = field.FldMetaataType
    fieldBaseType.typeString

    """   
    def set""" + field.Name.capitalize + """(value: """ + field.FieldTypePhysicalName + """): Unit = {
     	fields("""" + field.Name + """") = (""" + fldsMap(field.FieldTypePhysicalName) + """, value);
    }
   """
  }

  /**
   * Set method for struct type
   */
  private def setMethodForStructMapped(field: Element): String = {

    val fieldBaseType: BaseTypeDef = field.FldMetaataType
    fieldBaseType.typeString

    """   
    def set""" + field.Name.capitalize + """(value: """ + field.FieldTypePhysicalName + """): Unit = {
     	fields("""" + field.Name + """") = (-1, value);
    }
   """
  }
  /**
   * Get method of scalar types for mapped messages
   */
  private def getMethodForScalarMapped(field: Element): String = {

    """    
    def get""" + field.Name.capitalize + """: """ + field.FieldTypePhysicalName + """= {
      val fldVal = fields.getOrElse("""" + field.Name + """", null)
        if(fldVal == null)
          return """ + field.FieldTypeImplementationName + """.Input(null);
      return """ + field.FieldTypeImplementationName + """.Input(fields("""" + field.Name + """")._2.toString);
    }          
    """

  }

  /*
   * Get method of Array types for Mapped messages
   */

  private def getMethodArrayMapped(field: Element): String = {
    val fieldBaseType: BaseTypeDef = field.FldMetaataType
    var arrayType: ArrayTypeDef = null
    arrayType = fieldBaseType.asInstanceOf[ArrayTypeDef]
    var returnStr: String = ""

    log.info("11111111111111******************" + arrayType.tType + "-----" + arrayType.elemDef.tType + "------------" + arrayType.elemDef.tTypeType.toString())

    if (arrayType.elemDef.tTypeType.toString().equalsIgnoreCase("tscalar")) {
      val fname = arrayType.elemDef.implementationName + ".Input"
      returnStr = """    
     def get""" + field.Name.capitalize + """: """ + field.FieldTypePhysicalName + """ = {
    var ret: """ + field.FieldTypePhysicalName + """ = """ + field.FieldTypePhysicalName + """()
    if (fields.contains("""" + field.Name + """")) {
      val arr = fields.getOrElse("""" + field.Name + """", null)._2
      if (arr != null) {
        val arrFld = CollectionAsArrString(arr)
        ret = arrFld.map(v => { """ + fname + """(v.toString) }).toArray
      }
    }
    return ret
  }     
    """
    } else if (arrayType.elemDef.tTypeType.toString().equalsIgnoreCase("tcontainer")) {
      val fname = arrayType.elemDef.PhysicalName
      log.info("11111111111111******************" + arrayType.tType + "-----" + arrayType.elemDef.tType + "------------" + arrayType.elemDef.tTypeType.toString() + "-----------" + fname)

      returnStr = """    
     def get""" + field.Name.capitalize + """: """ + field.FieldTypePhysicalName + """ = {
    var ret: """ + field.FieldTypePhysicalName + """ = """ + field.FieldTypePhysicalName + """()
    if (fields.contains("""" + field.Name + """")) {
      val arr = fields.getOrElse("""" + field.Name + """", null)._2.asInstanceOf[""" + field.FieldTypePhysicalName + """]
      if (arr != null) {
        ret = arr.map(v => {  v.asInstanceOf[""" + fname + """]}).toArray
      }
    }
    return ret
  }     
    """
    }

    return returnStr

  }
  /*
   * Get method of Array Buffer types for Mapped messages
   */

  private def getMethodArrayBufMapped(field: Element): String = {
    val fieldBaseType: BaseTypeDef = field.FldMetaataType
    var arrayBufType: ArrayBufTypeDef = null
    arrayBufType = fieldBaseType.asInstanceOf[ArrayBufTypeDef]
    var returnStr: String = ""
    if (arrayBufType.elemDef.tTypeType.toString().equalsIgnoreCase("tscalar")) {
      val fname = arrayBufType.elemDef.implementationName + ".Input"

      returnStr = """    
    def get""" + field.Name.capitalize + """: """ + field.FieldTypePhysicalName + """ = {
      var ret: """ + field.FieldTypePhysicalName + """ =  new """ + field.FieldTypePhysicalName + """
      if (fields.contains("""" + field.Name + """")) {
        val arr = fields.getOrElse("""" + field.Name + """", null)._2.asInstanceOf[""" + field.FieldTypePhysicalName + """]
        if (arr != null) {
          val arrFld = arr.toArray
          arrFld.map(v => { ret :+=""" + fname + """(v.toString) }).toArray
        }
      }
      return ret
    }     
    """
    } else if (arrayBufType.elemDef.tTypeType.toString().equalsIgnoreCase("tcontainer")) {
      val fname = arrayBufType.elemDef.PhysicalName
      returnStr = """    
  def get""" + field.Name.capitalize + """: """ + field.FieldTypePhysicalName + """ = {
    var ret: """ + field.FieldTypePhysicalName + """ =  new """ + field.FieldTypePhysicalName + """
    if (fields.contains("""" + field.Name + """")) {
      val arr = fields.getOrElse("""" + field.Name + """", null)._2.asInstanceOf[""" + field.FieldTypePhysicalName + """]
      if (arr != null) {
        val arrFld = arr.toArray
        arrFld.map(v => { ret :+= v.asInstanceOf[""" + fname + """]}).toArray
      }
    }
    return ret
  }     
    """
    }
    return returnStr
  }

  /**
   * Get Method for the message/container type
   */

  private def getMethodForStructType(field: Element, mdMgr: MdMgr): String = {
    var ctrDef: ContainerDef = mdMgr.Container(field.Ttype, -1, true).getOrElse(null)
    if (ctrDef != null) {
      return """    
    def get""" + field.Name.capitalize + """: """ + field.FieldTypePhysicalName + """  = {
      var ret : """ + field.FieldTypePhysicalName + """  = null
      if (fields.contains("""" + field.Name + """")) {
        var id = fields.getOrElse("""" + field.Name + """", null)
        if (id != null)
          ret = fields("""" + field.Name + """")._2.asInstanceOf[""" + field.FieldTypePhysicalName + """];
       }
      return ret
    }
    """
    } else return ""
  }

  /*
   * Get Method generation function for Mapped Messages
   */

  def getFuncGenerationForMapped(fields: List[Element], mdMgr: MdMgr): String = {

    getFuncGenForMapped(fields, mdMgr)
  }

  private def getFuncGenForMapped(fields: List[Element], mdMgr: MdMgr): String = {
    var getMethod = new StringBuilder(8 * 1024)
    var getmethodStr: String = ""
    try {
      fields.foreach(field => {

        var getmethodStr: String = ""
        val fieldBaseType: BaseTypeDef = field.FldMetaataType
        val fieldType = fieldBaseType.tType.toString().toLowerCase()
        val fieldTypeType = fieldBaseType.tTypeType.toString().toLowerCase()

        // log.info("fieldTypeType " + fieldTypeType)
        // log.info("fieldBaseType 1 " + fieldBaseType.tType)
        // log.info("fieldBaseType 2 " + fieldBaseType.typeString)
        // log.info("fieldBaseType 3" + fieldBaseType.tTypeType)

        //  log.info("fieldType " + fieldType)

        fieldTypeType match {

          case "tscalar" => {
            getmethodStr = getMethodForScalarMapped(field)
            // log.info("fieldBaseType.implementationName    " + fieldBaseType.implementationName)
          }
          case "tcontainer" => {
            fieldType match {
              case "tarray" => {
                getmethodStr = getMethodArrayMapped(field)
              }
              case "tarraybuf" => {
                getmethodStr = getMethodArrayBufMapped(field)
              }
              case "tstruct" => {
                getmethodStr = getMethodForStructType(field, mdMgr)
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
        getMethod = getMethod.append(getmethodStr.toString())
        // log.info("=========SET Methods ===============" + getMethod.toString());
      })
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        log.debug("StackTrace:" + stackTrace)
        throw e
      }
    }
    return getMethod.toString
  }

}