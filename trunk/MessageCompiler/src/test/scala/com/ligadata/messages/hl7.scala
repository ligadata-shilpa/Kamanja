package com.ligadata.messages

import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.Formats
import com.ligadata.KamanjaBase._;
import com.ligadata.BaseTypes._
import java.io.{ DataInputStream, DataOutputStream, ByteArrayOutputStream }
import com.ligadata.Exceptions.StackTrace
import org.apache.logging.log4j.{ Logger, LogManager }
import scala.collection.mutable.ArrayBuffer;
import java.util.Date

object hl7 extends RDDObject[hl7] with ContainerFactoryInterface {
  type T = hl7;

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

  def getTimePartitionInfo: TimePartitionInfo = {
    var timePartitionInfo: TimePartitionInfo = new TimePartitionInfo();
    timePartitionInfo.setFieldName("clm_from_dt");
    timePartitionInfo.setFormat("epochtime");
    timePartitionInfo.setTimePartitionType(TimePartitionInfo.TimePartitionType.DAILY)
    return timePartitionInfo
  }

  override def getContainerType: ContainerFactoryInterface.ContainerType = ContainerFactoryInterface.ContainerType.MESSAGE
  override def isFixed: Boolean = false
  override def getAvroSchema: String = ""
  override def getSchemaId = 0;
  override def getPrimaryKeyNames: Array[String] = {
    var primaryKeyNames: Array[String] = Array("desynpuf_id", "clm_id");
    return primaryKeyNames;
  }

  override def getPartitionKeyNames: Array[String] = {
    var partitionKeyNames: Array[String] = Array("desynpuf_id");
    return partitionKeyNames;
  }

  override def createInstance: ContainerInterface = null
  override def getFullTypeName: String = "System.HL7"
  override def getTypeNameSpace: String = "System"
  override def getTypeName: String = "HL7"
  override def getTypeVersion: String = "000000.000001.000000"
  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this);
  override def getFullName: String = ""
  override def build: T = null
  override def build(from: T): T = null
}

class hl7(factory: ContainerFactoryInterface) extends ContainerInterface(factory) {
  val logger = this.getClass.getName
  lazy val log = LogManager.getLogger(logger)

  var valuesMap = Array[AttributeValue]()

  val attributeTypes = Array(new AttributeTypeInfo(0, 1, 0, 0, "string"), new AttributeTypeInfo(1, 1, 0, 0, "long"), new AttributeTypeInfo(2, 1, 0, 0, "int"), new AttributeTypeInfo(3, 1, 0, 0, "int"), new AttributeTypeInfo(4, 1, 0, 0, "int"), new AttributeTypeInfo(5, 1, 0, 0, "int"), new AttributeTypeInfo(6, 1, 0, 0, "int"), new AttributeTypeInfo(7, 1, 0, 0, "int"));

  val keyTypes: Map[String, AttributeTypeInfo] = Map("desynpuf_id" -> attributeTypes(0), "clm_id" -> attributeTypes(1), "clm_from_dt" -> attributeTypes(2), "clm_thru_dt" -> attributeTypes(3), "bene_birth_dt" -> attributeTypes(4), "bene_death_dt" -> attributeTypes(4), "bene_sex_ident_cd" -> attributeTypes(5), "bene_race_cd" -> attributeTypes(6))

  override def getPrimaryKey(): Array[String] = {
    var primaryKeys: scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer[String]();
    try {
      primaryKeys += com.ligadata.BaseTypes.StringImpl.toString(get("desynpuf_id").asInstanceOf[String]);
      primaryKeys += com.ligadata.BaseTypes.LongImpl.toString(get("clm_id").asInstanceOf[Long]);
    } catch {
      case e: Exception => {
        log.debug("", e.getStackTrace)
        throw e
      }
    }
    primaryKeys.toArray;
  }

  override def getPartitionKey(): Array[String] = {
    var partitionKeys: scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer[String]();
    try {
      partitionKeys += com.ligadata.BaseTypes.StringImpl.toString(get("desynpuf_id").asInstanceOf[String]);
    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    }
    partitionKeys.toArray;
  }

  override def save() = {}
  override def Clone(): ContainerOrConcept = { hl7.build(this) }

  override def getAttributeNames(): Array[String] = {
    var attributeNames: scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer[String]();
    try {
      val iter = valuesMap.iterator;
      while (iter.hasNext) {
        val attributeName = iter.next().getName;
        if (attributeName != null && attributeName.trim() != "")
          attributeNames += attributeName;
      }
    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    }
    return attributeNames.toArray;
  }

  override def get(key: String): Any = { // Return (value, type)
    return valuesMap(valuesMap.indexOf(key)).getValue
  }

  override def getOrElse(key: String, defaultVal: Any): Any = { // Return (value, type)
    try {
      val value = valuesMap(valuesMap.indexOf(key)).getValue
      if (value == null) return defaultVal;
      return value;
    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    }
  }

  override def get(index: Int): Any = { // Return (value, type)
    throw new Exception("Get By Index is not supported in mapped messages");
  }

  override def getOrElse(index: Int, defaultVal: Any): Any = { // Return (value,  type)
    throw new Exception("Get By Index is not supported in mapped messages");
  }

  override def getAllAttributeValues(): Array[AttributeValue] = { // Has (name, value, type))
    return valuesMap;
  }

  override def getAttributeNameAndValueIterator(): java.util.Iterator[AttributeValue] = {
    //valuesMap.iterator.asInstanceOf[java.util.Iterator[AttributeValue]];
    return null;
  }

  override def set(key: String, value: Any) = {
    try {
      val keyName: String = key.toLowerCase();
      if (keyTypes.contains(key)) {
        val valType = keyTypes.get(keyName)
        valuesMap :+ new AttributeValue(key, value, keyTypes(keyName))
      } else {
        // valuesMap :+ new AttributeValue(key, value, keyTypes(keyName))   -- Set the AttrubutesTypeInfo

      }

    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    }
  }

  override def set(key: String, value: Any, valTyp: String) = {
    try {
    //  valuesMap :+ new AttributeValue(key, value, keyTypes(key))  ... Need to get more details...
    } catch {
      case e: Exception => {
        log.debug("", e)
        throw e
      }
    }
  }

  override def set(index: Int, value: Any) = {
    throw new Exception("Set By Index is not supported in mapped messages");
  }
}

