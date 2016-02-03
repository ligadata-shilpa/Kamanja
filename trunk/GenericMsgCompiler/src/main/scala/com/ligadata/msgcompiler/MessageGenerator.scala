package com.ligadata.msgcompiler

import com.ligadata.Exceptions._;
import com.ligadata.Exceptions.StackTrace;
import org.apache.logging.log4j.{ Logger, LogManager }
import com.ligadata.kamanja.metadata._;

class MessageGenerator {

  var builderGenerator = new MessageBuilderGenerator
  var msgObjectGenerator = new MessageObjectGenerator
  var mappedMsgGen = new MappedMsgGenerator
  var msgConstants = new MessageConstants
  val logger = this.getClass.getName
  lazy val log = LogManager.getLogger(logger)

  /*
   * Generate the versioned and non versioned class for both mapped and Fixed messages
 	 * add import stamts -- still need to add
   * Generate Message Class
   * Message Class lines generation
   * Generate all the getter methods in the class generation
   * Generation all the setter methods in class generation
   * 
   */
  def generateMessage(message: Message, mdMgr: MdMgr): (String, String) = {

    generateMsg(message, mdMgr)
  }

  private def generateMsg(message: Message, mdMgr: MdMgr): (String, String) = {

    var messageVerGenerator = new StringBuilder(8 * 1024)
    var messageNonVerGenerator = new StringBuilder(8 * 1024)
    var messageGenerator = new StringBuilder(8 * 1024)

    try {
      if (message.Fixed.equalsIgnoreCase("true")) {

        messageVerGenerator = messageVerGenerator.append(msgConstants.newline + msgConstants.packageStr.format(message.Pkg, msgConstants.newline));
        messageNonVerGenerator = messageNonVerGenerator.append(msgConstants.newline + msgConstants.packageStr.format(message.NameSpace + msgConstants.newline));
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

        messageVerGenerator = messageVerGenerator.append(messageGenerator.toString())
        messageNonVerGenerator = messageNonVerGenerator.append(messageGenerator.toString())

      } else if (message.Fixed.equalsIgnoreCase("false")) {
        var fieldIndexMap: Map[String, Int] = msgConstants.getScalarFieldindex(message.Elements)

        messageVerGenerator = messageVerGenerator.append(msgConstants.newline + msgConstants.packageStr.format(message.Pkg, msgConstants.newline));
        messageNonVerGenerator = messageNonVerGenerator.append(msgConstants.newline + msgConstants.packageStr.format(message.NameSpace, msgConstants.newline));
        messageGenerator = messageGenerator.append(msgConstants.importStatements + msgConstants.newline);
        messageGenerator = messageGenerator.append(msgObjectGenerator.generateMessageObject(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(classGen(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(getMessgeBasicDetails(message))
        messageGenerator = messageGenerator.append(methodsFromBaseMsg(message))
        messageGenerator = messageGenerator.append(messageContructor(message))
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + mappedMsgGen.primayPartitionKeysVar(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + hasPartitionKeyFunc + msgConstants.newline)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + hasPrimaryKeysFunc + msgConstants.newline)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + partitionKeys(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + primaryKeys(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + generateParitionKeysData(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + generatePrimaryKeysData(message) + msgConstants.newline)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + generateSchema(message))
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + msgConstants.fieldsForMappedVar)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + keysVarforMapped(message.Elements))
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + msgConstants.getByNameFuncForMapped)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + msgConstants.getOrElseFuncForMapped)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + msgConstants.setByNameFuncForMappedMsgs)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + mappedMsgGen.setFuncGenerationforMapped(message.Elements, fieldIndexMap, mdMgr))
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + mappedMsgGen.getFuncGenerationForMapped(message.Elements, mdMgr))
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + msgConstants.CollectionAsArrString)
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.pad1 + mappedMsgGen.AddArraysInConstructor(message.Elements))
        messageGenerator = messageGenerator.append(builderMethod)
        messageGenerator = messageGenerator.append(builderGenerator.generatorBuilder(message))
        messageGenerator = messageGenerator.append(msgConstants.newline + msgConstants.closeBrace)

        messageVerGenerator = messageVerGenerator.append(messageGenerator.toString())
        messageNonVerGenerator = messageNonVerGenerator.append(messageGenerator.toString())

      }

     // log.info("messageGenerator    " + messageGenerator.toString())

    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        log.debug("StackTrace:" + stackTrace)
        throw e
      }
    }
    return (messageVerGenerator.toString, messageNonVerGenerator.toString())
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
      //log.info("constructor Generation ===================" + msgStr.substring(0, msgStr.length() - 1))
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
  private def getByName(key: String): Any = null
  private def getWithReflection(key: String): Any = null
  override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = {}
  override def GetMessage(childPath: Array[(String, String)], primaryKey: Array[String]): com.ligadata.KamanjaBase.BaseMsg = null
  def populate(inputdata: InputData) = {}
  private def populateCSV(inputdata: DelimitedData): Unit = {}
  private def populateJson(json: JsonData): Unit = {}
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

  private def partitionKeys(message: Message): String = {

    var paritionKeysStr: String = ""
    if (message.PartitionKeys == null || message.PartitionKeys.size == 0) {
      paritionKeysStr = "val partitionKeys: Array[String] = Array[String](); ";
    } else {
      paritionKeysStr = "val partitionKeys = Array(" + message.PartitionKeys.map(p => { " \"" + p.toLowerCase + "\"" }).mkString(", ") + ");";

    }

    return paritionKeysStr

  }

  private def primaryKeys(message: Message): String = {

    var primaryKeysStr: String = ""
    if (message.PrimaryKeys == null || message.PrimaryKeys.size == 0) {
      primaryKeysStr = "val primaryKeys: Array[String] = Array[String](); ";
    } else {
      primaryKeysStr = "val primaryKeys: Array[String] = Array(" + message.PrimaryKeys.map(p => { "\"" + p.toLowerCase + "\"" }).mkString(", ") + ");";

    }

    return primaryKeysStr

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

  /*
   * handling system colums in Mapped Messages
   */

  private def getSystemColumns(): String = {

    return ""

  }

  /*
   * generate keys variable for mapped message
   */
  private def keysVarforMapped(fields: List[Element]): String = {
    var mappedTypesABuf = new scala.collection.mutable.ArrayBuffer[String]
    var baseTypId = -1
    var firstTimeBaseType: Boolean = true
    var keysStr = new StringBuilder(8 * 1024)
    val stringType = MdMgr.GetMdMgr.Type("System.String", -1, true)
    if (stringType.getOrElse("None").equals("None"))
      throw new Exception("Type not found in metadata for String ")
    mappedTypesABuf += stringType.get.implementationName

    fields.foreach(field => {
      var typstring = field.FieldTypePhysicalName
      println("field.FieldTypePhysicalName   " + field.FieldTypePhysicalName)
      println("field.   " + field.FldMetaataType)
      if (mappedTypesABuf.contains(typstring)) {
        if (mappedTypesABuf.size == 1 && firstTimeBaseType)
          baseTypId = mappedTypesABuf.indexOf(typstring)
      } else {
        mappedTypesABuf += typstring
        baseTypId = mappedTypesABuf.indexOf(typstring)
      }
      keysStr.append("(\"" + field.Name + "\", " + mappedTypesABuf.indexOf(typstring) + "),")

    })
    //log.info(keysStr.toString().substring(0, keysStr.toString().length - 1))
    var keys = "var keys = Map(" + keysStr.toString().substring(0, keysStr.toString().length - 1) + ")";
    return keys
  }

  /*var typstring = field.ttyp.get.implementationName
      
      
     

        keysStr.append("(\"" + f.Name + "\", " + mappedTypesABuf.indexOf(typstring) + "),")
        
       */

  /*
    def getKeysStr(keysStr: String) = {
    if (keysStr != null && keysStr.trim() != "") {

      """ 
      var keys = Map(""" + keysStr.toString.substring(0, keysStr.toString.length - 1) + ") \n " +
        """
      var fields: scala.collection.mutable.Map[String, (Int, Any)] = new scala.collection.mutable.HashMap[String, (Int, Any)];
 	"""
    } else {
      """ 
	    var keys = Map[String, Int]()
	    var fields: scala.collection.mutable.Map[String, (Int, Any)] = new scala.collection.mutable.HashMap[String, (Int, Any)];
	  
	  """
    }
  }
    */
}