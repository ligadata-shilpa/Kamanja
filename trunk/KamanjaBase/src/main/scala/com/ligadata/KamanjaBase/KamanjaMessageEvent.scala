package com.ligadata.KamanjaBase

;

import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.Formats
import scala.xml.XML
import scala.xml.Elem
import com.ligadata.KamanjaBase.{InputData, DelimitedData, JsonData, XmlData, KvData}
import com.ligadata.BaseTypes._
import java.io.{DataInputStream, DataOutputStream, ByteArrayOutputStream}
import org.apache.logging.log4j.{Logger, LogManager}
import java.util.Date
import com.ligadata.KamanjaBase.{BaseMsg, BaseMsgObj, BaseContainer, MdBaseResolveInfo, MessageContainerBase, RDDObject, RDD, JavaRDDObject}

object KamanjaMessageEvent extends RDDObject[KamanjaMessageEvent] with BaseMsgObj {

  override def NeedToTransformData: Boolean = false

  override def FullName: String = "com.ligadata.KamanjaBase.KamanjaMessageEvent"

  override def NameSpace: String = "com.ligadata.KamanjaBase"

  override def Name: String = "KamanjaMessageEvent"

  override def Version: String = "000001.000005.000000"

  override def CreateNewMessage: BaseMsg = new KamanjaMessageEvent()

  override def IsFixed: Boolean = true;

  override def IsKv: Boolean = false;

  override def CanPersist: Boolean = false;


  type T = KamanjaMessageEvent

  override def build = new T

  override def build(from: T) = new T(from.transactionId, from)

  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this)


  val partitionKeys: Array[String] = Array[String]();

  override def PartitionKeyData(inputdata: InputData): Array[String] = Array[String]()

  val primaryKeys: Array[String] = Array[String]();

  override def PrimaryKeyData(inputdata: InputData): Array[String] = Array[String]()

  def getTimePartitionInfo: (String, String, String) = {
    // Column, Format & Types
    return (null, null, null)
  }

  override def TimePartitionData(inputdata: InputData): Long = 0


  override def hasPrimaryKey(): Boolean = {
    if (primaryKeys == null) return false;
    (primaryKeys.size > 0);
  }

  override def hasPartitionKey(): Boolean = {
    if (partitionKeys == null) return false;
    (partitionKeys.size > 0);
  }

  override def hasTimeParitionInfo(): Boolean = {
    val tmPartInfo = getTimePartitionInfo
    (tmPartInfo != null && tmPartInfo._1 != null && tmPartInfo._2 != null && tmPartInfo._3 != null);
  }


  override def getFullName = FullName

}

class KamanjaMessageEvent(var transactionId: Long, other: KamanjaMessageEvent) extends BaseMsg {
  override def IsFixed: Boolean = KamanjaMessageEvent.IsFixed;

  override def IsKv: Boolean = KamanjaMessageEvent.IsKv;

  override def CanPersist: Boolean = KamanjaMessageEvent.CanPersist;

  override def FullName: String = KamanjaMessageEvent.FullName

  override def NameSpace: String = KamanjaMessageEvent.NameSpace

  override def Name: String = KamanjaMessageEvent.Name

  override def Version: String = KamanjaMessageEvent.Version


  var messageid: Long = _;
  var modelinfo: scala.Array[com.ligadata.KamanjaBase.KamanjaModelEvent] = scala.Array[com.ligadata.KamanjaBase.KamanjaModelEvent]();
  var elapsedtimeinms: Float = _;
  var messagekey: String = _;
  var messagevalue: String = _;
  var error: String = _;

  override def PartitionKeyData: Array[String] = Array[String]()

  override def PrimaryKeyData: Array[String] = Array[String]()

  override def set(key: String, value: Any): Unit = {
    throw new Exception("set function is not yet implemented")
  }

  override def get(key: String): Any = {
    try {
      // Try with reflection
      return getWithReflection(key)
    } catch {
      case e: Exception => {
        LOG.debug("", e)
        // Call By Name
        return getByName(key)
      }
    }
  }

  override def getOrElse(key: String, default: Any): Any = {
    throw new Exception("getOrElse function is not yet implemented")
  }

  private def getByName(key: String): Any = {
    try {
      if (key.equals("messageid")) return messageid;
      if (key.equals("modelinfo")) return modelinfo;
      if (key.equals("elapsedtimeinms")) return elapsedtimeinms;
      if (key.equals("messagekey")) return messagekey;
      if (key.equals("messagevalue")) return messagevalue;
      if (key.equals("error")) return error;
      if (key.equals("transactionId")) return transactionId;

      if (key.equals("timePartitionData")) return timePartitionData;
      return null;
    } catch {
      case e: Exception => {
        LOG.debug("", e)
        throw e
      }
    }
  }

  private def getWithReflection(key: String): Any = {
    val ru = scala.reflect.runtime.universe
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val im = m.reflect(this)
    val fieldX = ru.typeOf[KamanjaMessageEvent].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
    val fmX = im.reflectField(fieldX)
    fmX.get
  }

  private val LOG = LogManager.getLogger(getClass)

  override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = {}


  override def GetMessage(childPath: Array[(String, String)], primaryKey: Array[String]): com.ligadata.KamanjaBase.BaseMsg = {
    return null
  }


  override def Save: Unit = {
    KamanjaMessageEvent.saveOne(this)
  }

  var nativeKeyMap = scala.collection.mutable.Map[String, String](("messageid", "messageId"), ("modelinfo", "ModelInfo"), ("elapsedtimeinms", "ElapsedTimeInMs"), ("messagekey", "messageKey"), ("messagevalue", "messageValue"), ("error", "Error"))

  override def getNativeKeyValues(): scala.collection.immutable.Map[String, (String, Any)] = {

    var keyValues: scala.collection.mutable.Map[String, (String, Any)] = scala.collection.mutable.Map[String, (String, Any)]()

    try {
      keyValues("messageid") = ("messageId", messageid);
      keyValues("modelinfo") = ("ModelInfo", modelinfo);
      keyValues("elapsedtimeinms") = ("ElapsedTimeInMs", elapsedtimeinms);
      keyValues("messagekey") = ("messageKey", messagekey);
      keyValues("messagevalue") = ("messageValue", messagevalue);
      keyValues("error") = ("Error", error);

    } catch {
      case e: Exception => {
        LOG.debug("", e)
        throw e
      }
    }
    return keyValues.toMap
  }

  override def Serialize(dos: DataOutputStream): Unit = {}

  override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = {}

  def ConvertPrevToNewVerObj(obj: Any): Unit = {}

  def withmessageid(value: Long): KamanjaMessageEvent = {
    this.messageid = value
    return this
  }

  def withmodelinfo(value: scala.Array[com.ligadata.KamanjaBase.KamanjaModelEvent]): KamanjaMessageEvent = {
    this.modelinfo = value
    return this
  }

  def withelapsedtimeinms(value: Float): KamanjaMessageEvent = {
    this.elapsedtimeinms = value
    return this
  }

  def withmessagekey(value: String): KamanjaMessageEvent = {
    this.messagekey = value
    return this
  }

  def withmessagevalue(value: String): KamanjaMessageEvent = {
    this.messagevalue = value
    return this
  }

  def witherror(value: String): KamanjaMessageEvent = {
    this.error = value
    return this
  }

  private def fromFunc(other: KamanjaMessageEvent): KamanjaMessageEvent = {
    return this
  }

  def ComputeTimePartitionData: Long = {
    val tmPartInfo = KamanjaMessageEvent.getTimePartitionInfo
    if (tmPartInfo == null) return 0;
    KamanjaMessageEvent.ComputeTimePartitionData(" ", tmPartInfo._2, tmPartInfo._3)
  }

  def getTimePartition(): KamanjaMessageEvent = {
    timePartitionData = ComputeTimePartitionData
    return this;
  }


  if (other != null && other != this) {
    // call copying fields from other to local variables
    fromFunc(other)
  }

  def this(txnId: Long) = {
    this(txnId, null)
  }

  def this(other: KamanjaMessageEvent) = {
    this(0, other)
  }

  def this() = {
    this(0, null)
  }

  override def Clone(): MessageContainerBase = {
    KamanjaMessageEvent.build(this)
  }


  override def hasPrimaryKey(): Boolean = {
    KamanjaMessageEvent.hasPrimaryKey;
  }

  override def hasPartitionKey(): Boolean = {
    KamanjaMessageEvent.hasPartitionKey;
  }

  override def hasTimeParitionInfo(): Boolean = {
    KamanjaMessageEvent.hasTimeParitionInfo;
  }

}