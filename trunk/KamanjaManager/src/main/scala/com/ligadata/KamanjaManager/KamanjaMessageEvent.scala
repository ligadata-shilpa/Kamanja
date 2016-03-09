package com.ligadata.KamanjaManager

import com.ligadata.KamanjaBase._
import java.io.DataInputStream
import java.io.DataOutputStream


/**
 *  This is the "MESSAGE" that will contain all the message0-context statistics.
 */

object KamanjaMessageEvent extends RDDObject[KamanjaMessageEvent] with BaseMsgObj {
  type T = KamanjaMessageEvent

  override def TransformDataAttributes: TransformMessage = null
  override def NeedToTransformData: Boolean = false
  override def FullName: String = "com.ligadata.KamanjaManager.KamanjaMessageEvent"
  override def NameSpace: String = "com.ligadata.KamanjaManager"
  override def Name: String = "KamanjaMessageEvent"
  override def Version: String = "000000.000000.000001"
  override def CreateNewMessage: BaseMsg = new KamanjaMessageEvent()
  override def IsFixed: Boolean = true
  override def IsKv: Boolean = false
  override def CanPersist: Boolean = false

  val partitionKeys: Array[String] = null
  val partKeyPos = Array(0)
  val primaryKeys: Array[String] = null

  def build = new T
  def build(from: T) = new T(from)

  override def PartitionKeyData(inputdata: InputData): Array[String] = Array[String]()
  override def PrimaryKeyData(inputdata: InputData): Array[String] = Array[String]()
  override def getTimePartitionInfo: (String, String, String) = (null, null, null) // FieldName, Format & Time Partition Types(Daily/Monthly/Yearly)
  override def TimePartitionData(inputdata: InputData): Long = 0

  override def hasPrimaryKey(): Boolean = {
    if(primaryKeys == null) return false;
    (primaryKeys.size > 0);
  }

  override def hasPartitionKey(): Boolean = {
    if(partitionKeys == null) return false;
    (partitionKeys.size > 0);
  }

  override def hasTimeParitionInfo(): Boolean = {
    val tmPartInfo = getTimePartitionInfo
    (tmPartInfo != null && tmPartInfo._1 != null && tmPartInfo._2 != null && tmPartInfo._3 != null);
  }

  override def getFullName = FullName
  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this)
}


class KamanjaMessageEvent(var transactionId: Long, other: KamanjaMessageEvent) extends BaseMsg {

  // These fields contain actual information
  var messageName: String = _
  var messageVersion: String = _
  var totalElapsedTime: Long = _
  var modelInfo: scala.collection.Map[String,(Long,Int)] = scala.collection.Map[String,(Long,Int)]()
  var error: String = _

  override def toString: String = {
    return messageName + "." + messageVersion + " -> " + totalElapsedTime
  }

  if (other != null && other != this) {
    // call copying fields from other to local variables
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

  def Clone(): MessageContainerBase = {
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

  override def IsFixed: Boolean = KamanjaMessageEvent.IsFixed;
  override def IsKv: Boolean = KamanjaMessageEvent.IsKv;
  override def CanPersist: Boolean = KamanjaMessageEvent.CanPersist;
  override def FullName: String = KamanjaMessageEvent.FullName
  override def NameSpace: String = KamanjaMessageEvent.NameSpace
  override def Name: String = KamanjaMessageEvent.Name
  override def Version: String = KamanjaMessageEvent.Version

  override def Save: Unit = {}
  override def PartitionKeyData: Array[String] = null
  override def PrimaryKeyData: Array[String] = null
  override def set(key: String, value: Any): Unit = {}
  override def get(key: String): Any = null
  override def getOrElse(key: String, default: Any): Any = { throw new Exception("getOrElse function is not yet implemented") }
  override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = {}
  override def GetMessage(childPath: Array[(String, String)], primaryKey: Array[String]): com.ligadata.KamanjaBase.BaseMsg = null
  override def Serialize(dos: DataOutputStream): Unit = {}
  override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = {}
  override def getNativeKeyValues(): scala.collection.immutable.Map[String, (String, Any)] = null

  def populate(inputdata: InputData) = {}
  def CollectionAsArrString(v: Any): Array[String] = null
  def ConvertPrevToNewVerObj(obj: Any): Unit = {}


  private def getByName(key: String): Any = null
  private def getWithReflection(key: String): Any = null
  private def populateCSV(inputdata: DelimitedData): Unit = {}
  private def populateJson(json: JsonData): Unit = {}
  private def assignJsonData(json: JsonData): Unit = {}
  private def populateXml(xmlData: XmlData): Unit = {}

}
