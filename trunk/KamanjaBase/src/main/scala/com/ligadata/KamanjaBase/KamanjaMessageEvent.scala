package com.ligadata.KamanjaBase
import java.io.{DataInputStream, DataOutputStream}
import org.apache.logging.log4j.{Logger, LogManager}

object KamanjaMessageEvent extends RDDObject[KamanjaMessageEvent] with MessageFactoryInterface {
  override def hasPrimaryKey: Boolean = false

  override def hasPartitionKey: Boolean = false

  override def hasTimePartitionInfo: Boolean = false

  override def getTimePartitionInfo: TimePartitionInfo = null

  override def getContainerType: ContainerFactoryInterface.ContainerType = ContainerFactoryInterface.ContainerType.MESSAGE

  override def isFixed: Boolean = true

  override def getSchema: String = ""

  override def getPrimaryKeyNames: Array[String] = Array[String]()

  override def getPartitionKeyNames: Array[String] = Array[String]()

  override def createInstance: ContainerInterface = new KamanjaMessageEvent(this)

  override def getFullName: String = getFullTypeName
  override def getFullTypeName: String = ""

  override def getTypeNameSpace: String = ""

  override def getTypeName: String = ""

  override def getTypeVersion: String = ""

  override def getSchemaId: Int = 101

  override def build  = new KamanjaMessageEvent(this)

  override def build(from: T)  = new KamanjaMessageEvent(this)

  type T = KamanjaMessageEvent

  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this)
}

class KamanjaMessageEvent(factory: MessageFactoryInterface) extends MessageInterface(factory) {
  var messageid: Long = _;
  var modelinfo: scala.Array[com.ligadata.KamanjaBase.KamanjaModelEvent] = scala.Array[com.ligadata.KamanjaBase.KamanjaModelEvent]();
  var elapsedtimeinms: Float = _;
  var messagekey: String = _;
  var messagevalue: String = _;
  var error: String = _;

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

  var nativeKeyMap = scala.collection.mutable.Map[String, String](("messageid", "messageId"), ("modelinfo", "ModelInfo"), ("elapsedtimeinms", "ElapsedTimeInMs"), ("messagekey", "messageKey"), ("messagevalue", "messageValue"), ("error", "Error"))

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
    return 0;
  }

  def getTimePartition(): KamanjaMessageEvent = {
    timePartitionData = ComputeTimePartitionData
    return this;
  }


  override def Clone(): ContainerInterface = {
    KamanjaMessageEvent.build(this)
  }

  override def getPrimaryKey: Array[String] = Array[String]()

  override def getPartitionKey: Array[String] = Array[String]()

  override def save: Unit = {
    KamanjaMessageEvent.saveOne(this)
  }

  override def getAttributeNames: Array[String] = Array[String]()

  override def get(key: String): AttributeValue = null

  override def get(index: Int): AttributeValue = null

  override def getOrElse(key: String, defaultVal: AnyRef): AttributeValue = null

  override def getOrElse(index: Int, defaultVal: AnyRef): AttributeValue = null

  override def getAllAttributeValues: java.util.HashMap[String, AttributeValue] = null

  override def set(key: String, value: AnyRef): Unit = {}

  override def set(index: Int, value: AnyRef): Unit = {}

  override def set(key: String, value: AnyRef, valTyp: String): Unit = {}

  override def getAttributeNameAndValueIterator: java.util.Iterator[java.util.Map.Entry[String, AttributeValue]] = null

}