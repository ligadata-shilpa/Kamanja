package com.ligadata.msgcompiler

import org.apache.logging.log4j.{ Logger, LogManager }

class MessageConstants {
  val logger = this.getClass.getName
  lazy val log = LogManager.getLogger(logger)

  val newline: String = "\n";
  val pad1: String = "\t";
  val pad2: String = "\t\t";
  val closeBrace: String = "}";
  val True: String = "true";
  val False: String = "false";
  val packageStr: String = "package %s; %s";
  val messageStr: String = "message";
  val containerStr: String = "container";
  val baseMsgObj: String = "BaseMsgObj";
  val baseMsg: String = "BaseMsg";
  val baseContainerObj: String = "BaseContainerObj";
  val baseContainer: String = "BaseContainer";
  val msgObjectStr: String = "object %s extends RDDObject[%s] with %s { %s";
  val classStr: String = "class %s(var transactionId: Long, other: %s) extends %s { %s";

  val template: String = "%s type T = %s ;%s";

  val fullName: String = "%s override def FullName: String = \"%s.%s\"; %s"; //com.ligadata.samples.messages.CustAlertHistory"
  val namespace: String = "%s override def NameSpace: String = \"%s\"; %s"; //com.ligadata.samples.messages"
  val name: String = "%s override def Name: String = \"%s\"; %s"; //CustAlertHistory"
  val version: String = "%s override def Version: String = \"%s\"; %s"; //000000.000001.000000"
  val createNewContainer = "%s override def CreateNewContainer: %s = new %s(); %s"; //BaseContainer = new CustAlertHistory()
  val createNewMessage = "%s override def CreateNewMessage: %s = new %s(); %s"; //BaseContainer = new CustAlertHistory()
  val isFixed: String = "%s override def IsFixed: Boolean = %s; %s"; //true;
  val isKV: String = "%s override def IsKv: Boolean = %s; %s"; //false;
  val canPersist: String = "%s override def CanPersist: Boolean = %s; %s"; //true;
  val getFullName: String = "%s override def getFullName = FullName; %s";
  val toJavaRDD: String = "%s override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this); %s";

  val paritionKeyData: String = "override def PartitionKeyData: Array[String] = %s ";
  val primaryKeyData: String = "override def PrimaryKeyData: Array[String] = %s ";
  val partitionKeys: String = "%s val partitionKeys : Array[String] = Array%s; %s";
  val primaryKeys: String = "%s val primaryKeys : Array[String] = Array%s; %s";
  val partitionKeyVar: String = "%s var partitionKeysData: scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer[String]();%s"
  val primaryKeyVar: String = "%s var primaryKeysData: scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer[String]();%s"

  val rddFactoryClass: String = "public final class %sFactory { %s";
  val rddObj: String = "public static JavaRDDObject<%s> rddObject = %s$.MODULE$.toJavaRDDObject(); %s";
  val rddBaseContainerObj = "public static BaseContainerObj baseObj = (BaseContainerObj) %s$.MODULE$; %s";
  val rddBaseMsgObj = "public static BaseMsgObj baseObj = (BaseMsgObj) %s$.MODULE$; %s";
  val fieldsForMappedVar = "var fields: scala.collection.mutable.Map[String, (Int, Any)] = new scala.collection.mutable.HashMap[String, (Int, Any)];"

  def rddObjImportStmts() = {
    """
  import com.ligadata.KamanjaBase.JavaRDDObject;
	import com.ligadata.KamanjaBase.BaseMsgObj;
	import com.ligadata.KamanjaBase.BaseContainerObj;
    """
  }

  def importStatements() = {
    """
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.Formats
import com.ligadata.KamanjaBase.{ InputData, DelimitedData, JsonData, XmlData, KvData }
import com.ligadata.BaseTypes._
import com.ligadata.KamanjaBase.SerializeDeserialize
import java.io.{ DataInputStream, DataOutputStream, ByteArrayOutputStream }
import com.ligadata.Exceptions.StackTrace;
import org.apache.logging.log4j.{ Logger, LogManager }
import java.util.Date
import com.ligadata.KamanjaBase.{ BaseMsg, BaseMsgObj, TransformMessage, BaseContainer, BaseContainerObj, MdBaseResolveInfo, MessageContainerBase, RDDObject, RDD, JavaRDDObject }
    
    """
  }

  def msgObjectBuildStmts = {
    """
  def build = new T
  def build(from: T) = new T(from)

    """
  }

  def getByNameFuncForMapped = {
    """
  override def get(key: String): Any = {
    fields.get(key) match {
      case Some(f) => {
        return f._2;
      }
      case None => {
        return null;
      }
    }
  }
"""
  }

  def getOrElseFuncForMapped = {
    """
  override def getOrElse(key: String, default: Any): Any = {
    fields.getOrElse(key, (-1, default))._2;
  }  
     
 """
  }

  def setByNameFuncForMappedMsgs() = {
    """
  override def set(key: String, value: Any): Unit = {
    if (key == null) throw new Exception(" key should not be null in set method")
    fields.put(key, (-1, value))
  }
"""
  }

  /*
   * Get the index for fields in mapped messages
   */
  def getScalarFieldindex(fields: List[Element]): Map[String, Int] = {
    var fldsSclarIndex: Map[String, Int] = Map(("String", 0))
    var setFields: Set[String] = Set[String]()
    var a = 0;
    var index: Int = 0
    for (a <- fields) {
      setFields = setFields + a.FieldTypePhysicalName
    }
    var b = 0;
    for (b <- setFields) {
      if (!b.equalsIgnoreCase("string"))
        fldsSclarIndex = fldsSclarIndex + ((b, index + 1))
    }
    log.info("fldsSclarIndex  fields " + fldsSclarIndex.toList)
    log.info("set fields " + setFields.toList)
    return fldsSclarIndex
  }

}