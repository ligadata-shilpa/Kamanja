package com.ligadata.msgcompiler

import com.ligadata.Exceptions._;
import com.ligadata.Exceptions.StackTrace;
import org.apache.logging.log4j.{ Logger, LogManager }

class MessageGenerator {

  var builderGenerator = new MessageBuilderGenerator
  var msgObjectGenerator = new MessageObjectGenerator
  var msgConstants = new MessageConstants
  val logger = this.getClass.getName
  lazy val log = LogManager.getLogger(logger)

  /*
   * add import stamts -- still need to add
   * Generate Message Class
   * Message Class lines generation
   * Generate all the getter methods in the class generation
   * Generation all the setter methods in class generation
   * 
   */
  def generateMessage(message: Message): String = {

    var messageGenerator = new StringBuilder(8 * 1024)
    try {
      if (message.Fixed.equalsIgnoreCase("true")) {

        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.packageStr.format(message.Pkg, msgConstants.newline));
        messageGenerator = messageGenerator.append(msgConstants.importStatements + msgConstants.newline);
        messageGenerator = messageGenerator.append(msgObjectGenerator.generateMessageObject(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(classGen(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(getMessgeBasicDetails(message))
        messageGenerator = messageGenerator.append(methodsFromBaseMsg(message))
        messageGenerator = messageGenerator.append(msgConstants.newline + partitionKeys(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(msgConstants.newline + primaryKeys(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(msgConstants.newline + generateParitionKeysData(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(msgConstants.newline + generatePrimaryKeysData(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(messageContructor(message))
        //messageGenerator = messageGenerator.append(msgClassConstructorGen(message))
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + generateSchema(message))
        messageGenerator = messageGenerator.append(msgConstants.newline + generatedMsgVariables(message))
        messageGenerator = messageGenerator.append(getFuncGeneration(message.Elements))
        messageGenerator = messageGenerator.append(setFuncGeneration(message.Elements))
        messageGenerator = messageGenerator.append(getFuncByOffset(message.Elements))
        messageGenerator = messageGenerator.append(setFuncByOffset(message.Elements))
        messageGenerator = messageGenerator.append(builderMethod)
        messageGenerator = messageGenerator.append(builderGenerator.generatorBuilder(message))
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.closeBrace)

      } else if (message.Fixed.equalsIgnoreCase("false")) {
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.packageStr.format(message.Pkg, msgConstants.newline));
        messageGenerator = messageGenerator.append(msgConstants.importStatements + msgConstants.newline);
        messageGenerator = messageGenerator.append(msgObjectGenerator.generateMessageObject(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(classGen(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(getMessgeBasicDetails(message))
        messageGenerator = messageGenerator.append(methodsFromBaseMsg(message))
        messageGenerator = messageGenerator.append(messageContructor(message))
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + hasPartitionKeyFunc + msgConstants.newline)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + hasPrimaryKeysFunc + msgConstants.newline)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + partitionKeys(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + primaryKeys(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + generateParitionKeysData(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + generatePrimaryKeysData(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + generateSchema(message))
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + msgConstants.fieldsForMappedVar)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + msgConstants.getByNameFuncForMapped)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + msgConstants.getOrElseFuncForMapped)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + msgConstants.setByNameFuncForMappedMsgs)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + setFuncGenerationforMapped(message.Elements))
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + getFuncGenerationForMapped(message.Elements))
        messageGenerator = messageGenerator.append(builderGenerator.generatorBuilder(message))
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.closeBrace)

      }

      log.info("messageGenerator    " + messageGenerator.toString())

    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        log.debug("StackTrace:" + stackTrace)
        throw e
      }
    }
    return messageGenerator.toString
  }
  /*
   * Message Class
   */
  private def classGen(message: Message): String = {
    var baseMsgType: String = ""
    val msgType = message.MsgType
    if (msgType == null || msgType.trim() == "")
      throw new Exception("Message Definition root element should be either Message or Container")

    if (msgType.equalsIgnoreCase(msgConstants.messageStr))
      baseMsgType = msgConstants.baseMsg
    else if (msgType.equalsIgnoreCase(msgConstants.containerStr))
      baseMsgType = msgConstants.baseContainer

    // (var transactionId: Long, other: CustAlertHistory) extends BaseContainer {
    return msgConstants.classStr.format(message.Name, message.Name, baseMsgType, msgConstants.newline)

  }

  /*
   * Generate the variables for the message 
   */
  private def generatedMsgVariables(message: Message): String = {
    var msgVariables = new StringBuilder(8 * 1024)
    try {
      message.Elements.foreach(field => {
        msgVariables.append(" %s private var %s: %s = _; %s".format(msgConstants.pad1, field.Name, field.FieldTypePhysicalName, msgConstants.newline))
      })
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        log.debug("StackTrace:" + stackTrace)
        throw e
      }
    }
    msgVariables.toString()
  }

  /*
   * Message constructor generation
   */

  private def msgConstructor(message: Message, msgStr: String, constructorStmts: String): String = {
    var msgClassConsGen: String = ""

    msgClassConsGen = """
        def """ + message.Name + """(""" + msgStr.substring(0, msgStr.length() - 2) + """) {
    """ + constructorStmts + """
        }"""

    return msgClassConsGen
  }

  /*
   * Message Class Constructor Generation
   */

  private def msgClassConstructorGen(message: Message): String = {
    var msgClassConsGen: String = ""
    var msgConsStr = new StringBuilder(8 * 1024)
    var constructorStmts = new StringBuilder(8 * 1024)

    try {
      message.Elements.foreach(element => {
        msgConsStr.append("%s: %s, ".format(element.Name, element.FieldTypePhysicalName))
        constructorStmts.append("%s this.%s = %s; %s ".format(msgConstants.pad2, element.Name, element.Name, msgConstants.newline))
      })
      val msgStr = msgConsStr.toString
      log.info("constructor Generation ===================" + msgStr.substring(0, msgStr.length() - 1))
      msgClassConsGen = msgConstructor(message, msgStr, constructorStmts.toString)
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        log.debug("StackTrace:" + stackTrace)
        throw e
      }
    }
    return msgClassConsGen.toString
  }

  /*
   * Get Method generation function for Fixed Messages
   */
  private def getFuncGeneration(fields: List[Element]): String = {
    var getMethod = new StringBuilder(8 * 1024)
    var getmethodStr: String = ""
    try {
      fields.foreach(field => {
        getmethodStr = """
        def get""" + field.Name.capitalize + """: """ + field.FieldTypePhysicalName + """= {
        	return this.""" + field.Name + """;
        }          
        """
        getMethod = getMethod.append(getmethodStr.toString())
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

  /*
   * Set Method Generation Function for Fixed Messages
   */
  private def setFuncGeneration(fields: List[Element]): String = {
    var setMethod = new StringBuilder(8 * 1024)
    var setmethodStr: String = ""
    try {
      fields.foreach(field => {
        setmethodStr = """
        def set""" + field.Name.capitalize + """(value: """ + field.FieldTypePhysicalName + """): Unit = {
        	this.""" + field.Name + """ = value;
        }
        """
        setMethod = setMethod.append(setmethodStr.toString())
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

  /*
   * Get By Ordinal Function generation
   */
  private def getFuncByOffset(fields: List[Element]): String = {
    var getFuncByOffset: String = ""
    getFuncByOffset = """
      def getByPosition(field$ : Int) : Any = {
      	field$ match {
  """ + getByOffset(fields) + """
      	 case _ => throw new Exception("Bad index");
    	}
      }      
    """
    return getFuncByOffset
  }

  /*
   * Get By Ordinal Function generation
   */
  private def getByOffset(fields: List[Element]): String = {
    var getByOffset = new StringBuilder(8 * 1024)
    try {
      fields.foreach(field => {
        getByOffset.append("%s case %s => return this.%s; %s".format(msgConstants.pad1, field.FieldOrdinal, field.Name, msgConstants.newline))
      })
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        log.debug("StackTrace:" + stackTrace)
        throw e
      }
    }
    return getByOffset.toString
  }

  /*
   * Set By Ordinal Function generation
   */
  private def setFuncByOffset(fields: List[Element]): String = {
    var getFuncByOffset: String = ""
    getFuncByOffset = """
      def setByPosition(field$ : Int, value :Any): Unit = {
      	field$ match {
  """ + setByOffset(fields) + """
      	 case _ => throw new Exception("Bad index");
    	}
      }      
    """
    return getFuncByOffset
  }

  /*
   * Set By Ordinal Function generation
   */
  private def setByOffset(fields: List[Element]): String = {
    var setByOffset = new StringBuilder(8 * 1024)
    try {
      fields.foreach(field => {
        setByOffset.append("%s case %s => {this.%s = value.asInstanceOf[%s]}; %s".format(msgConstants.pad1, field.FieldOrdinal, field.Name, field.FieldTypePhysicalName, msgConstants.newline))
      })
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        log.debug("StackTrace:" + stackTrace)
        throw e
      }
    }
    return setByOffset.toString
  }

  private def messageContructor(message: Message): String = {
    """
   def this(txnId: Long) = {
    this(txnId, null)
  }
  def this(other: """ + message.Name + """) = {
    this(0, other)
  }
  def this() = {
    this(0, null)
  }
   
    
  """
  }

  /*
   * message basic details in class
   */
  private def getMessgeBasicDetails(message: Message): String = {
    """ 
  override def IsFixed: Boolean = """ + message.Name + """.IsFixed;
  override def IsKv: Boolean = """ + message.Name + """.IsKv;
  override def CanPersist: Boolean = """ + message.Name + """.CanPersist;
  override def FullName: String = """ + message.Name + """.FullName
  override def NameSpace: String = """ + message.Name + """.NameSpace
  override def Name: String = """ + message.Name + """.Name
  override def Version: String = """ + message.Name + """.Version
  """
  }

  /*
   * some overridable methods from BaseMsg
   */
  private def methodsFromBaseMsg(message: Message): String = {
    """
  override def Save: Unit = { """ + message.Name + """.saveOne(this) }
  override def set(key: String, value: Any): Unit = {}
  override def get(key: String): Any = null
  override def getOrElse(key: String, default: Any): Any = { throw new Exception("getOrElse function is not yet implemented") }
  private def getByName(key: String): Any = null
  private def getWithReflection(key: String): Any = null
  override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = {}
  override def GetMessage(childPath: Array[(String, String)], primaryKey: Array[String]): com.ligadata.KamanjaBase.BaseMsg = null
  def populate(inputdata: InputData) = {}
  private def populateCSV(inputdata: DelimitedData): Unit = {}
  private def populateJson(json: JsonData): Unit = {}
  def CollectionAsArrString(v: Any): Array[String] = null
  private def assignJsonData(json: JsonData): Unit = {}
  private def populateXml(xmlData: XmlData): Unit = {}
  override def Serialize(dos: DataOutputStream): Unit = {}
  override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = {}
  def ConvertPrevToNewVerObj(obj: Any): Unit = {}
  override def getNativeKeyValues(): scala.collection.immutable.Map[String, (String, Any)] = null
  
  override def hasTimeParitionInfo(): Boolean = {
    """ + message.Name + """.hasTimeParitionInfo;
  }
    
  def Clone(): MessageContainerBase = {
     """ + message.Name + """.build(this)
  }
    """
  }

  /*
    override def PartitionKeyData(): Array[String] = {
    var partitionKeysData: scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer[String]()
    partitionKeysData += com.ligadata.BaseTypes.StringImpl.toString(getField1)
    partitionKeysData += com.ligadata.BaseTypes.LongImpl.toString(getField2)
    partitionKeysData.toArray
  }

  val PartitionKeys: Array[String] = Array("field1", "field2")
   
   */

  /*
   * parititon keys code generation
   */

  private def generateParitionKeysData(message: Message): String = {
    var paritionKeysGen = new StringBuilder(8 * 1024)
    var returnPartitionKeyStr: String = ""
    val arryOfStr: String = "Array[String]()";

    if (message.PartitionKeys != null && message.PartitionKeys.size > 0) {
      paritionKeysGen.append("{" + msgConstants.newline)
      paritionKeysGen.append(msgConstants.partitionKeyVar.format(msgConstants.pad1, msgConstants.newline))
      message.PartitionKeys.foreach(key => {
        message.Elements.foreach(element => {
          if (element.Name.equalsIgnoreCase(key)) {
            paritionKeysGen.append("%s partitionKeysData += %s.toString(get%s);%s".format(msgConstants.pad1, element.FldMetaataType.implementationName, element.Name.capitalize, msgConstants.newline)) //"+ com.ligadata.BaseTypes.StringImpl+".toString(get"+element.Name.capitalize+") ")
          }
        })
      })
      paritionKeysGen.append("%s partitionKeysData.toArray; %s".format(msgConstants.pad1, msgConstants.newline))
      paritionKeysGen.append("%s } %s".format(msgConstants.pad1, msgConstants.newline))

      returnPartitionKeyStr = msgConstants.paritionKeyData.format(paritionKeysGen.toString)
    } else {
      returnPartitionKeyStr = msgConstants.paritionKeyData.format(arryOfStr)
    }

    returnPartitionKeyStr
  }

  /*
   * primary keys code generation
   */
  private def generatePrimaryKeysData(message: Message): String = {
    var primaryKeysGen = new StringBuilder(8 * 1024)
    var returnPrimaryKeyStr: String = ""
    val arryOfStr: String = "Array[String]()";

    if (message.PrimaryKeys != null && message.PrimaryKeys.size > 0) {
      primaryKeysGen.append("{" + msgConstants.newline)
      primaryKeysGen.append(msgConstants.primaryKeyVar.format(msgConstants.pad1, msgConstants.newline))
      message.PrimaryKeys.foreach(key => {
        message.Elements.foreach(element => {
          if (element.Name.equalsIgnoreCase(key)) {
            primaryKeysGen.append("%s primaryKeysData += %s.toString(get%s);%s".format(msgConstants.pad1, element.FldMetaataType.implementationName, element.Name.capitalize, msgConstants.newline)) //"+ com.ligadata.BaseTypes.StringImpl+".toString(get"+element.Name.capitalize+") ")
          }
        })
      })
      primaryKeysGen.append("%s primaryKeysData.toArray; %s".format(msgConstants.pad1, msgConstants.newline))
      primaryKeysGen.append("%s } %s".format(msgConstants.pad1, msgConstants.newline))
      returnPrimaryKeyStr = msgConstants.primaryKeyData.format(primaryKeysGen.toString)
    } else {
      returnPrimaryKeyStr = msgConstants.primaryKeyData.format(arryOfStr)
    }
    returnPrimaryKeyStr
  }

  /*
   * Builder method in the message definition class
   */

  private def builderMethod(): String = {
    """
    def newBuilder(): Builder = {
    	return new Builder();
    }
    
    """

  }

  /*
   * Generate the schema String of the input message/container
   */
  private def generateSchema(message: Message): String = {

    return "val schema: String = \" " + message.Schema + " \" ;"

  }

  /* 
 * SetByName for the mapped messages
 */
  private def setByNameFuncForMappedMsgs(): String = {

    return ""
  }

  /*
   * SetByName for the Fixed Messages
   */
  private def setByNameFuncForFixedMsgs(): String = {

    return ""
  }

  /*
   * GetByName for mapped Messages
   */
  private def getByNameFuncForMapped(): String = {

    return ""
  }

  /*
   * GetByName merthod for Fixed Messages
   */
  private def getByNameFuncForFixed: String = {

    return ""

  }

  /*
   * Set Method Generation Function for Mapped Messages
   */
  private def setFuncGenerationforMapped(fields: List[Element]): String = {
    var setMethod = new StringBuilder(8 * 1024)
    var setmethodStr: String = ""
    try {
      fields.foreach(field => {
        setmethodStr = """   def set""" + field.Name.capitalize + """(value: """ + field.FieldTypePhysicalName + """): Unit = {
        	fields("""" + field.Name + """") = value;
        }
        """
        setMethod = setMethod.append(setmethodStr.toString())
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

  /*
   * Get Method generation function for Mapped Messages
   */
  private def getFuncGenerationForMapped(fields: List[Element]): String = {
    var getMethod = new StringBuilder(8 * 1024)
    var getmethodStr: String = ""
    try {
      fields.foreach(field => {
        getmethodStr = """    def get""" + field.Name.capitalize + """: """ + field.FieldTypePhysicalName + """= {
        	return fields("""" + field.Name + """");
        }          
        """
        getMethod = getMethod.append(getmethodStr.toString())
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

  private def partitionKeys(message: Message): String = {

    var paritionKeysStr: String = ""
    if (message.PartitionKeys != null || message.PartitionKeys.size > 0) {
      paritionKeysStr = "val partitionKeys = Array(" + message.PartitionKeys.map(p => { " \"" + p.toLowerCase + "\"" }).mkString(", ") + ");";
    } else {
      paritionKeysStr = "val partitionKeys: Array[String] = Array[String](); ";
    }

    return paritionKeysStr

  }

  private def primaryKeys(message: Message): String = {

    var primaryKeysStr: String = ""
    if (message.PrimaryKeys != null || message.PrimaryKeys.size > 0) {
      primaryKeysStr = "val primaryKeys: Array[String] = Array(" + message.PrimaryKeys.map(p => { "\"" + p.toLowerCase + "\"" }).mkString(", ") + ");";
    } else {
      primaryKeysStr = "val primaryKeys: Array[String] = Array[String](); ";
    }

    return primaryKeysStr

  }

  private def PartitionKeyData(message: Message): String = {

    var paritionKeysData: String = ""
    if (message.PartitionKeys == null || message.PartitionKeys.size == 0) {
      paritionKeysData = " override def PartitionKeyData(inputdata: InputData): Array[String] = Array[String](); ";
    } else {
      paritionKeysData = " override def PartitionKeyData(inputdata: InputData): Array[String] = Array[String](); ";
    }

    return paritionKeysData

  }

  private def primaryKeyData(message: Message): String = {

    var primaryKeysData: String = ""
    if (message.PrimaryKeys == null || message.PrimaryKeys.size == 0) {
      primaryKeysData = "override def PrimaryKeyData(inputdata: InputData): Array[String] = Array[String]()";
    } else {
      primaryKeysData = "override def PrimaryKeyData(inputdata: InputData): Array[String] = Array[String]() ";
    }
    return primaryKeysData

  }

  private def hasPrimaryKeysFunc() = {
    """
  override def hasPrimaryKey(): Boolean = {
    if (primaryKeys == null) return false;
    (primaryKeys.size > 0);
  }
  """
  }

  private def hasPartitionKeyFunc() = {
    """
  override def hasPartitionKey(): Boolean = {
    if (partitionKeys == null) return false;
    (partitionKeys.size > 0);
  }
  """
  }

}