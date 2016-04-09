
package com.ligadata.KamanjaBase.V1000002000000;

import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.Formats
import com.ligadata.KamanjaBase.{ AttributeTypeInfo, AttributeValue, ContainerFactoryInterface, ContainerInterface, MessageFactoryInterface, MessageInterface, TimePartitionInfo, ContainerOrConceptFactory, RDDObject, JavaRDDObject, ContainerOrConcept }
import com.ligadata.BaseTypes._
import com.ligadata.Exceptions.StackTrace;
import org.apache.logging.log4j.{ Logger, LogManager }
import java.util.Date

object KamanjaStatisticsEvent extends RDDObject[KamanjaStatisticsEvent] with MessageFactoryInterface {
  type T = KamanjaStatisticsEvent;
  override def getFullTypeName: String = "com.ligadata.KamanjaBase.KamanjaStatisticsEvent";
  override def getTypeNameSpace: String = "com.ligadata.KamanjaBase";
  override def getTypeName: String = "KamanjaStatisticsEvent";
  override def getTypeVersion: String = "000001.000002.000000";
  override def getSchemaId: Int = 1000006;
  override def createInstance: KamanjaStatisticsEvent = new KamanjaStatisticsEvent(KamanjaStatisticsEvent);
  override def isFixed: Boolean = true;
  override def getContainerType: ContainerFactoryInterface.ContainerType = ContainerFactoryInterface.ContainerType.MESSAGE
  override def getFullName = getFullTypeName;
  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this);

  def build = new T(this)
  def build(from: T) = new T(from)
  override def getPartitionKeyNames: Array[String] = Array[String]();

  override def getPrimaryKeyNames: Array[String] = Array[String]();

  override def getTimePartitionInfo: TimePartitionInfo = { return null; } // FieldName, Format & Time Partition Types(Daily/Monthly/Yearly)

  override def hasPrimaryKey(): Boolean = {
    val pKeys = getPrimaryKeyNames();
    return (pKeys != null && pKeys.length > 0);
  }

  override def hasPartitionKey(): Boolean = {
    val pKeys = getPartitionKeyNames();
    return (pKeys != null && pKeys.length > 0);
  }

  override def hasTimePartitionInfo(): Boolean = {
    val tmInfo = getTimePartitionInfo();
    return (tmInfo != null && tmInfo.getTimePartitionType != TimePartitionInfo.TimePartitionType.NONE);
  }

  override def getAvroSchema: String = """{ "type": "record",  "namespace" : "com.ligadata.kamanjabase" , "name" : "kamanjastatisticsevent" , "fields":[{ "name" : "statistics" , "type" : "string"}]}""";
}

class KamanjaStatisticsEvent(factory: MessageFactoryInterface, other: KamanjaStatisticsEvent) extends MessageInterface(factory) {

  private val log = LogManager.getLogger(getClass)

  var keyTypes: Map[String, AttributeTypeInfo] = attributeTypes.map { a => (a.getName, a) }.toMap;

  if (other != null && other != this) {
    // call copying fields from other to local variables
    fromFunc(other)
  }

  override def save: Unit = { /* KamanjaStatisticsEvent.saveOne(this) */ }

  def Clone(): ContainerOrConcept = { KamanjaStatisticsEvent.build(this) }

  override def getPartitionKey: Array[String] = Array[String]()

  override def getPrimaryKey: Array[String] = Array[String]()

  var attributeTypes = generateAttributeTypes;

  private def generateAttributeTypes(): Array[AttributeTypeInfo] = {
    var attributeTypes = new Array[AttributeTypeInfo](1);
    attributeTypes :+ new AttributeTypeInfo("statistics", 0, AttributeTypeInfo.TypeCategory.STRING, 1, 1, 0)

    return attributeTypes
  }

  var statistics: String = _;

  override def getAttributeTypes(): Array[AttributeTypeInfo] = {
    if (attributeTypes == null) return null;
    return attributeTypes
  }
  
  private def getWithReflection(key: String): AnyRef = {
    val ru = scala.reflect.runtime.universe
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val im = m.reflect(this)
    val fieldX = ru.typeOf[KamanjaStatisticsEvent].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
    val fmX = im.reflectField(fieldX)
    return fmX.get.asInstanceOf[AnyRef];
  }

  override def get(key: String): AnyRef = {
    try {
      // Try with reflection
      return getByName(key.toLowerCase())
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        log.debug("StackTrace:" + stackTrace)
        // Call By Name
        return getWithReflection(key.toLowerCase())
      }
    }
  }

  private def getByName(key: String): AnyRef = {
    if (!keyTypes.contains(key)) throw new Exception(s"Key $key does not exists in message/container hl7Fixed ");
    return get(keyTypes(key).getIndex)
  }

  override def getOrElse(key: String, defaultVal: Any): AnyRef = { 
    try {
      val value = get(key.toLowerCase())
      if (value == null) return defaultVal.asInstanceOf[AnyRef]; else return value;
    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    }
    return null;
  }

   def getOrElse(index: Int, defaultVal: Any): AnyRef = { 
    try {
      val value = get(index)
      if (value == null) return defaultVal.asInstanceOf[AnyRef]; else return value;
    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    }
    return null;
  }

  override def getAttributeNames(): Array[String] = {
    try {
      if (keyTypes.isEmpty) {
        return null;
      } else {
        return keyTypes.keySet.toArray;
      }
    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    }
    return null;
  }

  override def getAllAttributeValues(): Array[AttributeValue] = { // Has ( value, attributetypeinfo))
    var attributeVals = new Array[AttributeValue](1);
    try {
      attributeVals :+ new AttributeValue(this.statistics, keyTypes("statistics"))

    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    };

    return attributeVals;
  }

  override def getAttributeNameAndValueIterator(): java.util.Iterator[AttributeValue] = {
    //getAllAttributeValues.iterator.asInstanceOf[java.util.Iterator[AttributeValue]];
    return null; // Fix - need to test to make sure the above iterator works properly

  }

  override def get(index: Int): AnyRef = { 
    try {
      index match {
        case 0 => return this.statistics;
        case _ => throw new Exception(s"$index is a bad index for message KamanjaStatisticsEvent");
      }
    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    };

  }

  override def set(key: String, value: Any) = {
    try {

      if (!keyTypes.contains(key)) throw new Exception(s"Key $key does not exists in message KamanjaStatisticsEvent")
      set(keyTypes(key).getIndex, value);

    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    };

  }

  def set(index: Int, value: Any): Unit = {
    if (value == null) throw new Exception(s"Value is null for index $index in message KamanjaStatisticsEvent ")
    try {
      index match {
        case 0 => {
          if (value.isInstanceOf[String])
            this.statistics = value.asInstanceOf[String];
          else throw new Exception(s"Value is the not the correct type for index $index in message KamanjaStatisticsEvent")
        }

        case _ => throw new Exception(s"$index is a bad index for message KamanjaStatisticsEvent");
      }
    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    };

  }

  override def set(key: String, value: Any, valTyp: String) = {
    throw new Exception("Set Func for Value and ValueType By Key is not supported for Fixed Messages")
  }

  private def fromFunc(other: KamanjaStatisticsEvent): KamanjaStatisticsEvent = {
    this.statistics = com.ligadata.BaseTypes.StringImpl.Clone(other.statistics);

    //this.timePartitionData = com.ligadata.BaseTypes.LongImpl.Clone(other.timePartitionData);
    return this;
  }

  def this(factory: MessageFactoryInterface) = {
    this(factory, null)
  }

  def this(other: KamanjaStatisticsEvent) = {
    this(other.getFactory.asInstanceOf[MessageFactoryInterface], other)
  }

}