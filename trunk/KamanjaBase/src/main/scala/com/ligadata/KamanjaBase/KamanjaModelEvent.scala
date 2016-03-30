package com.ligadata.KamanjaBase

import java.io.{DataInputStream, DataOutputStream}
import org.apache.logging.log4j.{Logger, LogManager}

object KamanjaModelEvent extends RDDObject[KamanjaModelEvent] with MessageFactoryInterface {
  override def hasPrimaryKey: Boolean = false

  override def hasPartitionKey: Boolean = false

  override def hasTimePartitionInfo: Boolean = false

  override def getTimePartitionInfo: TimePartitionInfo = null

  override def getContainerType: ContainerFactoryInterface.ContainerType = ContainerFactoryInterface.ContainerType.MESSAGE

  override def isFixed: Boolean = true

  override def getSchema: String = ""

  override def getPrimaryKeyNames: Array[String] = Array[String]()

  override def getPartitionKeyNames: Array[String] = Array[String]()

  override def createInstance: ContainerInterface = new KamanjaModelEvent(this)

  override def getFullName: String = getFullTypeName
  override def getFullTypeName: String = ""

  override def getTypeNameSpace: String = ""

  override def getTypeName: String = ""

  override def getTypeVersion: String = ""

  override def getSchemaId: Int = 102

  override def build  = new KamanjaModelEvent(this)

  override def build(from: T)  = new KamanjaModelEvent(this)

  type T = KamanjaModelEvent

  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this)
}

class KamanjaModelEvent(factory: MessageFactoryInterface) extends MessageInterface(factory) {

  var modelid: Long = _;
  var elapsedtimeinms: Float = _;
  var eventepochtime: Long = _;
  var isresultproduced: Boolean = _;
  var producedmessages: scala.Array[Long] = _;
  var error: String = _;

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
    return 0;
  }

  def getTimePartition(): KamanjaModelEvent = {
    timePartitionData = ComputeTimePartitionData
    return this;
  }


  override def Clone(): ContainerInterface = {
    KamanjaModelEvent.build(this)
  }

  override def getPrimaryKey: Array[String] = Array[String]()

  override def getPartitionKey: Array[String] = Array[String]()

  override def save: Unit = {
    KamanjaModelEvent.saveOne(this)
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