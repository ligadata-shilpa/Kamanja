package com.ligadata.KamanjaBase
import java.io.{DataInputStream, DataOutputStream}
import org.apache.logging.log4j.{Logger, LogManager}

object KamanjaStatusEvent extends RDDObject[KamanjaStatusEvent] with MessageFactoryInterface {
  override def hasPrimaryKey: Boolean = false

  override def hasPartitionKey: Boolean = false

  override def hasTimePartitionInfo: Boolean = false

  override def getTimePartitionInfo: TimePartitionInfo = null

  override def getContainerType: ContainerFactoryInterface.ContainerType = ContainerFactoryInterface.ContainerType.MESSAGE

  override def isFixed: Boolean = true

  override def getSchema: String = ""

  override def getPrimaryKeyNames: Array[String] = Array[String]()

  override def getPartitionKeyNames: Array[String] = Array[String]()

  override def createInstance: ContainerInterface = new KamanjaStatusEvent(this)

  override def getFullName: String = getFullTypeName
  override def getFullTypeName: String = ""

  override def getTypeNameSpace: String = ""

  override def getTypeName: String = ""

  override def getTypeVersion: String = ""

  override def build  = new KamanjaStatusEvent(this)

  override def build(from: T)  = new KamanjaStatusEvent(this)

  type T = KamanjaStatusEvent

  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this)


}

class KamanjaStatusEvent(factory: MessageFactoryInterface) extends MessageInterface(factory) {


  var nodeid: String = _;
  var eventtime: Long = _;
  var statusstring: String = _;


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


  override def Clone(): ContainerInterface = {
    KamanjaStatusEvent.build(this)
  }

  override def getPrimaryKey: Array[String] = Array[String]()

  override def getPartitionKey: Array[String] = Array[String]()

  override def save: Unit = {
    KamanjaStatusEvent.saveOne(this)
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