package com.ligadata.KamanjaBase

import java.io.{DataInputStream, DataOutputStream}
import org.apache.logging.log4j.{Logger, LogManager}

object KamanjaModelEvent extends RDDObject[KamanjaModelEvent] with BaseMsgObj {
  override def NeedToTransformData: Boolean = false

  override def FullName: String = "com.ligadata.KamanjaBase.KamanjaModelEvent"

  override def NameSpace: String = "com.ligadata.KamanjaBase"

  override def Name: String = "KamanjaModelEvent"

  override def Version: String = "000001.000002.000000"

  override def CreateNewMessage: BaseMsg = new KamanjaModelEvent()

  override def IsFixed: Boolean = true;

  override def IsKv: Boolean = false;

  override def CanPersist: Boolean = false;


  type T = KamanjaModelEvent

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

class KamanjaModelEvent(var transactionId: Long, other: KamanjaModelEvent) extends BaseMsg {
  override def IsFixed: Boolean = KamanjaModelEvent.IsFixed;

  override def IsKv: Boolean = KamanjaModelEvent.IsKv;

  override def CanPersist: Boolean = KamanjaModelEvent.CanPersist;

  override def FullName: String = KamanjaModelEvent.FullName

  override def NameSpace: String = KamanjaModelEvent.NameSpace

  override def Name: String = KamanjaModelEvent.Name

  override def Version: String = KamanjaModelEvent.Version


  var modelid: Long = _;
  var elapsedtimeinms: Float = _;
  var eventepochtime: Long = _;
  var isresultproduced: Boolean = _;
  var producedmessages: scala.Array[Long] = _;
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
      if (key.equals("modelid")) return modelid;
      if (key.equals("elapsedtimeinms")) return elapsedtimeinms;
      if (key.equals("eventepochtime")) return eventepochtime;
      if (key.equals("isresultproduced")) return isresultproduced;
      if (key.equals("producedmessages")) return producedmessages;
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
    val fieldX = ru.typeOf[KamanjaModelEvent].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
    val fmX = im.reflectField(fieldX)
    fmX.get
  }

  private val LOG = LogManager.getLogger(getClass)

  override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = {}


  override def GetMessage(childPath: Array[(String, String)], primaryKey: Array[String]): com.ligadata.KamanjaBase.BaseMsg = {
    return null
  }


  override def Save: Unit = {
    KamanjaModelEvent.saveOne(this)
  }

  var nativeKeyMap = scala.collection.mutable.Map[String, String](("modelid", "modelId"), ("elapsedtimeinms", "ElapsedTimeInMs"), ("eventepochtime", "eventEpochTime"), ("isresultproduced", "isResultProduced"), ("producedmessages", "ProducedMessages"), ("error", "Error"))

  override def getNativeKeyValues(): scala.collection.immutable.Map[String, (String, Any)] = {

    var keyValues: scala.collection.mutable.Map[String, (String, Any)] = scala.collection.mutable.Map[String, (String, Any)]()

    try {
      keyValues("modelid") = ("modelId", modelid);
      keyValues("elapsedtimeinms") = ("ElapsedTimeInMs", elapsedtimeinms);
      keyValues("eventepochtime") = ("eventEpochTime", eventepochtime);
      keyValues("isresultproduced") = ("isResultProduced", isresultproduced);
      keyValues("producedmessages") = ("ProducedMessages", producedmessages);
      keyValues("error") = ("Error", error);

    } catch {
      case e: Exception => {
        LOG.debug("", e)
        throw e
      }
    }
    return keyValues.toMap
  }

  def ConvertPrevToNewVerObj(obj: Any): Unit = {}

  def withmodelid(value: Long): KamanjaModelEvent = {
    this.modelid = value
    return this
  }

  def withelapsedtimeinms(value: Float): KamanjaModelEvent = {
    this.elapsedtimeinms = value
    return this
  }

  def witheventepochtime(value: Long): KamanjaModelEvent = {
    this.eventepochtime = value
    return this
  }

  def withisresultproduced(value: Boolean): KamanjaModelEvent = {
    this.isresultproduced = value
    return this
  }

  def withproducedmessages(value: scala.Array[Long]): KamanjaModelEvent = {
    this.producedmessages = value
    return this
  }

  def witherror(value: String): KamanjaModelEvent = {
    this.error = value
    return this
  }

  private def fromFunc(other: KamanjaModelEvent): KamanjaModelEvent = {
    return this
  }

  def ComputeTimePartitionData: Long = {
    val tmPartInfo = KamanjaModelEvent.getTimePartitionInfo
    if (tmPartInfo == null) return 0;
    KamanjaModelEvent.ComputeTimePartitionData(" ", tmPartInfo._2, tmPartInfo._3)
  }

  def getTimePartition(): KamanjaModelEvent = {
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

  def this(other: KamanjaModelEvent) = {
    this(0, other)
  }

  def this() = {
    this(0, null)
  }

  override def Clone(): MessageContainerBase = {
    KamanjaModelEvent.build(this)
  }


  override def hasPrimaryKey(): Boolean = {
    KamanjaModelEvent.hasPrimaryKey;
  }

  override def hasPartitionKey(): Boolean = {
    KamanjaModelEvent.hasPartitionKey;
  }

  override def hasTimeParitionInfo(): Boolean = {
    KamanjaModelEvent.hasTimeParitionInfo;
  }

  override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = {}

  override def Serialize(dos: DataOutputStream): Unit = {}

  override def populate(inputdata: InputData): Unit = {}
}