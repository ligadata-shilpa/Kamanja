package com.ligadata.KamanjaBase
import java.io.{DataInputStream, DataOutputStream}
import org.apache.logging.log4j.{Logger, LogManager}

object KamanjaStatusEvent extends RDDObject[KamanjaStatusEvent] with BaseMsgObj {
  override def NeedToTransformData: Boolean = false

  override def FullName: String = "com.ligadata.KamanjaBase.KamanjaStatusEvent"

  override def NameSpace: String = "com.ligadata.KamanjaBase"

  override def Name: String = "KamanjaStatusEvent"

  override def Version: String = "000001.000002.000000"

  override def CreateNewMessage: BaseMsg = new KamanjaStatusEvent()

  override def IsFixed: Boolean = true;

  override def IsKv: Boolean = false;

  override def CanPersist: Boolean = false;


  type T = KamanjaStatusEvent

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

class KamanjaStatusEvent(var transactionId: Long, other: KamanjaStatusEvent) extends BaseMsg {
  override def IsFixed: Boolean = KamanjaStatusEvent.IsFixed;

  override def IsKv: Boolean = KamanjaStatusEvent.IsKv;

  override def CanPersist: Boolean = KamanjaStatusEvent.CanPersist;

  override def FullName: String = KamanjaStatusEvent.FullName

  override def NameSpace: String = KamanjaStatusEvent.NameSpace

  override def Name: String = KamanjaStatusEvent.Name

  override def Version: String = KamanjaStatusEvent.Version


  var nodeid: String = _;
  var eventtime: Long = _;
  var statusstring: String = _;

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
      if (key.equals("nodeid")) return nodeid;
      if (key.equals("eventtime")) return eventtime;
      if (key.equals("statusstring")) return statusstring;
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
    val fieldX = ru.typeOf[KamanjaStatusEvent].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
    val fmX = im.reflectField(fieldX)
    fmX.get
  }

  private val LOG = LogManager.getLogger(getClass)

  override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = {}


  override def GetMessage(childPath: Array[(String, String)], primaryKey: Array[String]): com.ligadata.KamanjaBase.BaseMsg = {
    return null
  }


  override def Save: Unit = {
    KamanjaStatusEvent.saveOne(this)
  }

  var nativeKeyMap = scala.collection.mutable.Map[String, String](("nodeid", "NodeId"), ("eventtime", "eventTime"), ("statusstring", "StatusString"))

  override def getNativeKeyValues(): scala.collection.immutable.Map[String, (String, Any)] = {

    var keyValues: scala.collection.mutable.Map[String, (String, Any)] = scala.collection.mutable.Map[String, (String, Any)]()

    try {
      keyValues("nodeid") = ("NodeId", nodeid);
      keyValues("eventtime") = ("eventTime", eventtime);
      keyValues("statusstring") = ("StatusString", statusstring);

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

  def withnodeid(value: String): KamanjaStatusEvent = {
    this.nodeid = value
    return this
  }

  def witheventtime(value: Long): KamanjaStatusEvent = {
    this.eventtime = value
    return this
  }

  def withstatusstring(value: String): KamanjaStatusEvent = {
    this.statusstring = value
    return this
  }

  private def fromFunc(other: KamanjaStatusEvent): KamanjaStatusEvent = {
    return this
  }

  def ComputeTimePartitionData: Long = {
    val tmPartInfo = KamanjaStatusEvent.getTimePartitionInfo
    if (tmPartInfo == null) return 0;
    KamanjaStatusEvent.ComputeTimePartitionData(" ", tmPartInfo._2, tmPartInfo._3)
  }

  def getTimePartition(): KamanjaStatusEvent = {
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

  def this(other: KamanjaStatusEvent) = {
    this(0, other)
  }

  def this() = {
    this(0, null)
  }

  override def Clone(): MessageContainerBase = {
    KamanjaStatusEvent.build(this)
  }


  override def hasPrimaryKey(): Boolean = {
    KamanjaStatusEvent.hasPrimaryKey;
  }

  override def hasPartitionKey(): Boolean = {
    KamanjaStatusEvent.hasPartitionKey;
  }

  override def hasTimeParitionInfo(): Boolean = {
    KamanjaStatusEvent.hasTimeParitionInfo;
  }

  override def populate(inputdata: InputData): Unit = {}
}