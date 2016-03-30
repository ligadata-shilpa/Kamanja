package com.ligadata.KamanjaBase
import java.io.{DataInputStream, DataOutputStream}
import org.apache.logging.log4j.{Logger, LogManager}

object KamanjaExceptionEvent extends RDDObject[KamanjaExceptionEvent] with MessageFactoryInterface {
  override def hasPrimaryKey: Boolean = false

  override def hasPartitionKey: Boolean = false

  override def hasTimePartitionInfo: Boolean = false

  override def getTimePartitionInfo: TimePartitionInfo = null

  override def getContainerType: ContainerFactoryInterface.ContainerType = ContainerFactoryInterface.ContainerType.MESSAGE

  override def isFixed: Boolean = true

  override def getSchema: String = ""

  override def getPrimaryKeyNames: Array[String] = Array[String]()

  override def getPartitionKeyNames: Array[String] = Array[String]()

  override def createInstance: ContainerInterface = new KamanjaExceptionEvent(this)

  override def getFullName: String = getFullTypeName
  override def getFullTypeName: String = ""

  override def getTypeNameSpace: String = ""

  override def getTypeName: String = ""

  override def getTypeVersion: String = ""

  override def getSchemaId: Int = 100

  type T = KamanjaExceptionEvent

  override def build  = new KamanjaExceptionEvent(this)

  override def build(from: T)  = new KamanjaExceptionEvent(this)

  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this)
}

class KamanjaExceptionEvent(factory: MessageFactoryInterface) extends MessageInterface(factory) {
  var componentname: String = _;
  var timeoferrorepochms: Long = _;
  var errortype: String = _;
  var errorstring: String = _;

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

  var nativeKeyMap = scala.collection.mutable.Map[String, String](("componentname", "ComponentName"), ("timeoferrorepochms", "TimeOfErrorEpochMs"), ("errortype", "ErrorType"), ("errorstring", "ErrorString"))

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

  def getTimePartition(): KamanjaExceptionEvent = {
    timePartitionData = 0
    return this;
  }

  override def Clone(): ContainerInterface = {
    KamanjaExceptionEvent.build(this)
  }

  override def getPrimaryKey: Array[String] = Array[String]()

  override def getPartitionKey: Array[String] = Array[String]()

  override def save: Unit = {
    KamanjaExceptionEvent.saveOne(this)
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
