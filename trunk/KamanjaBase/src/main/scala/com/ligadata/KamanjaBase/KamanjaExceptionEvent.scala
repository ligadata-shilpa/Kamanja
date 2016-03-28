package com.ligadata.KamanjaBase
import java.io.{DataInputStream, DataOutputStream}
import org.apache.logging.log4j.{Logger, LogManager}

object KamanjaExceptionEvent extends RDDObject[KamanjaExceptionEvent] with BaseMsgObj {
  override def NeedToTransformData: Boolean = false

  override def FullName: String = "com.ligadata.KamanjaBase.KamanjaExceptionEvent"

  override def NameSpace: String = "com.ligadata.KamanjaBase"

  override def Name: String = "KamanjaExceptionEvent"

  override def Version: String = "000001.000002.000000"

  override def CreateNewMessage: BaseMsg = new KamanjaExceptionEvent()

  override def IsFixed: Boolean = true;

  override def IsKv: Boolean = false;

  override def CanPersist: Boolean = false;


  type T = KamanjaExceptionEvent

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

class KamanjaExceptionEvent(var transactionId: Long, other: KamanjaExceptionEvent) extends BaseMsg {
  override def IsFixed: Boolean = KamanjaExceptionEvent.IsFixed;

  override def IsKv: Boolean = KamanjaExceptionEvent.IsKv;

  override def CanPersist: Boolean = KamanjaExceptionEvent.CanPersist;

  override def FullName: String = KamanjaExceptionEvent.FullName

  override def NameSpace: String = KamanjaExceptionEvent.NameSpace

  override def Name: String = KamanjaExceptionEvent.Name

  override def Version: String = KamanjaExceptionEvent.Version


  var componentname: String = _;
  var timeoferrorepochms: Long = _;
  var errortype: String = _;
  var errorstring: String = _;

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
      if (key.equals("componentname")) return componentname;
      if (key.equals("timeoferrorepochms")) return timeoferrorepochms;
      if (key.equals("errortype")) return errortype;
      if (key.equals("errorstring")) return errorstring;
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
    val fieldX = ru.typeOf[KamanjaExceptionEvent].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
    val fmX = im.reflectField(fieldX)
    fmX.get
  }

  private val LOG = LogManager.getLogger(getClass)

  override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = {}


  override def GetMessage(childPath: Array[(String, String)], primaryKey: Array[String]): com.ligadata.KamanjaBase.BaseMsg = {
    return null
  }


  override def Save: Unit = {
    KamanjaExceptionEvent.saveOne(this)
  }

  var nativeKeyMap = scala.collection.mutable.Map[String, String](("componentname", "ComponentName"), ("timeoferrorepochms", "TimeOfErrorEpochMs"), ("errortype", "ErrorType"), ("errorstring", "ErrorString"))

  override def getNativeKeyValues(): scala.collection.immutable.Map[String, (String, Any)] = {

    var keyValues: scala.collection.mutable.Map[String, (String, Any)] = scala.collection.mutable.Map[String, (String, Any)]()

    try {
      keyValues("componentname") = ("ComponentName", componentname);
      keyValues("timeoferrorepochms") = ("TimeOfErrorEpochMs", timeoferrorepochms);
      keyValues("errortype") = ("ErrorType", errortype);
      keyValues("errorstring") = ("ErrorString", errorstring);

    } catch {
      case e: Exception => {
        LOG.debug("", e)
        throw e
      }
    }
    return keyValues.toMap
  }

  def ConvertPrevToNewVerObj(obj: Any): Unit = {}

  def withcomponentname(value: String): KamanjaExceptionEvent = {
    this.componentname = value
    return this
  }

  def withtimeoferrorepochms(value: Long): KamanjaExceptionEvent = {
    this.timeoferrorepochms = value
    return this
  }

  def witherrortype(value: String): KamanjaExceptionEvent = {
    this.errortype = value
    return this
  }

  def witherrorstring(value: String): KamanjaExceptionEvent = {
    this.errorstring = value
    return this
  }

  private def fromFunc(other: KamanjaExceptionEvent): KamanjaExceptionEvent = {
    return this
  }

  def ComputeTimePartitionData: Long = {
    val tmPartInfo = KamanjaExceptionEvent.getTimePartitionInfo
    if (tmPartInfo == null) return 0;
    KamanjaExceptionEvent.ComputeTimePartitionData(" ", tmPartInfo._2, tmPartInfo._3)
  }

  def getTimePartition(): KamanjaExceptionEvent = {
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

  def this(other: KamanjaExceptionEvent) = {
    this(0, other)
  }

  def this() = {
    this(0, null)
  }

  override def Clone(): MessageContainerBase = {
    KamanjaExceptionEvent.build(this)
  }


  override def hasPrimaryKey(): Boolean = {
    KamanjaExceptionEvent.hasPrimaryKey;
  }

  override def hasPartitionKey(): Boolean = {
    KamanjaExceptionEvent.hasPartitionKey;
  }

  override def hasTimeParitionInfo(): Boolean = {
    KamanjaExceptionEvent.hasTimeParitionInfo;
  }

  override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = {}

  override def Serialize(dos: DataOutputStream): Unit = {}

  override def populate(inputdata: InputData): Unit = {}

}
