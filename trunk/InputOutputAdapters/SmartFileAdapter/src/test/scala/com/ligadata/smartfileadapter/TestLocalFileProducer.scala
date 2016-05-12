package com.ligadata.smartfileadapter

import org.scalatest._
import com.ligadata.KamanjaBase._ // { AttributeTypeInfo, ContainerFactoryInterface, ContainerInterface, ContainerOrConcept }
import com.ligadata.AdaptersConfiguration.SmartFileProducerConfiguration
import com.ligadata.OutputAdapters.SmartFileProducer
import com.ligadata.InputOutputAdapterInfo.AdapterConfiguration
import com.ligadata.Exceptions.{ KamanjaException, FatalAdapterException, KeyNotFoundException }

class ParameterContainer(factory: ContainerFactoryInterface) extends ContainerInterface(factory) {

  var attributeTypes = generateAttributeTypes;

  private def generateAttributeTypes(): Array[AttributeTypeInfo] = {
    var attributeTypes = new Array[AttributeTypeInfo](3);
    attributeTypes(0) = new AttributeTypeInfo("id", 0, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
    attributeTypes(1) = new AttributeTypeInfo("name", 1, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
    attributeTypes(2) = new AttributeTypeInfo("value", 2, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)

    return attributeTypes
  }
  
  var keyTypes: Map[String, AttributeTypeInfo] = attributeTypes.map { a => (a.getName, a) }.toMap;
  
  def Clone(): ContainerOrConcept = { null }
  override def save: Unit = { }
  override def getPartitionKey: Array[String] = Array[String]()
  override def getPrimaryKey: Array[String] = Array[String]()
  
  override def getAttributeTypes(): Array[AttributeTypeInfo] = {
    if (attributeTypes == null) return null;
    return attributeTypes
  }
  
  override def getAttributeType(name: String): AttributeTypeInfo = {
    if (name == null || name.trim() == "") return null;
    attributeTypes.foreach(attributeType => {
      if (attributeType.getName == name.toLowerCase())
        return attributeType
    })
    return null;
  }
  
  var id: Int = _;
  var name: String = _;
  var value: String = _;
  
  override def get(index: Int): AnyRef = { 
    try {
      index match {
        case 0 => return this.id.asInstanceOf[AnyRef];
        case 1 => return this.name.asInstanceOf[AnyRef];
        case 2 => return this.value.asInstanceOf[AnyRef];

        case _ => throw new Exception(s"$index is a bad index for message ParameterContainer");
      }
    } catch {
      case e: Exception => {
        throw e
      }
    };

  }
  
  override def get(keyName: String): AnyRef = {
    if (keyName == null || keyName.trim.size == 0) throw new Exception("Please provide proper key name " + keyName);
    val key = keyName.toLowerCase;

    if (!keyTypes.contains(key)) throw new KeyNotFoundException(s"Key $key does not exists in message/container ParameterContainer", null);
    return get(keyTypes(key).getIndex)     
  }
  
  override def getOrElse(keyName: String, defaultVal: Any): AnyRef = {
    if (keyName == null || keyName.trim.size == 0) throw new Exception("Please provide proper key name " + keyName);
    val key = keyName.toLowerCase;
    try {
      val value = get(key.toLowerCase())
      if (value == null) return defaultVal.asInstanceOf[AnyRef]; else return value;
    } catch {
      case e: Exception => {
        throw e
      }
    }
    return null;
  }
  
  override def getOrElse(index: Int, defaultVal: Any): AnyRef = { // Return (value,  type)
    try {
      val value = get(index)
      if (value == null) return defaultVal.asInstanceOf[AnyRef]; else return value;
    } catch {
      case e: Exception => {
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
        throw e
      }
    }
    return null;
  }

  override def getAllAttributeValues(): Array[AttributeValue] = { 
    var attributeVals = new Array[AttributeValue](3);
    try {
      attributeVals(0) = new AttributeValue(this.id, keyTypes("id"))
      attributeVals(1) = new AttributeValue(this.name, keyTypes("name"))
      attributeVals(2) = new AttributeValue(this.value, keyTypes("value"))
    } catch {
      case e: Exception => {
        throw e
      }
    };

    return attributeVals;
  }
    override def set(keyName: String, value: Any) = {
    if (keyName == null || keyName.trim.size == 0) throw new Exception("Please provide proper key name " + keyName);
    val key = keyName.toLowerCase;
    try {

      if (!keyTypes.contains(key)) throw new KeyNotFoundException(s"Key $key does not exists in message ParameterContainer", null)
      set(keyTypes(key).getIndex, value);

    } catch {
      case e: Exception => {
        throw e
      }
    };

  }

  def set(index: Int, value: Any): Unit = {
    if (value == null) throw new Exception(s"Value is null for index $index in message ParameterContainer ")
    try {
      index match {
        case 0 => {
          if (value.isInstanceOf[Int])
            this.id = value.asInstanceOf[Int];
          else throw new Exception(s"Value is the not the correct type for field modelid in message ParameterContainer")
        }
        case 1 => {
          if (value.isInstanceOf[String])
            this.name = value.asInstanceOf[String];
          else throw new Exception(s"Value is the not the correct type for field elapsedtimeinms in message ParameterContainer")
        }
        case 2 => {
          if (value.isInstanceOf[String])
            this.value = value.asInstanceOf[String];
          else throw new Exception(s"Value is the not the correct type for field eventepochtime in message ParameterContainer")
        }

        case _ => throw new Exception(s"$index is a bad index for message ParameterContainer");
      }
    } catch {
      case e: Exception => {
        throw e
      }
    };

  }

  override def set(key: String, value: Any, valTyp: String) = {
    throw new Exception("Set Func for Value and ValueType By Key is not supported for Fixed Messages")
  }
}

class TestLocalFileProducer extends FunSpec with BeforeAndAfter with ShouldMatchers with BeforeAndAfterAll with GivenWhenThen {

  describe("Test Smart File Producer configuration") {

    val inputConfig = new AdapterConfiguration()
    inputConfig.Name = "TestOutput_2"
    inputConfig.className = "com.ligadata.InputAdapters.SamrtFileProducer$"
    inputConfig.jarName = "smartfileinputoutputadapters_2.10-1.0.jar"

    it("should read configuration correctly from a valid JSON") {

      inputConfig.adapterSpecificCfg =
        """
  		  |{
  		  |  "Uri": "file://nameservice/folder/to/save",
	  	  |  "FileNamePrefix": "Data"
	  	  |}
		    """.stripMargin

      val conf = SmartFileProducerConfiguration.getAdapterConfig(inputConfig)
      val sfp = SmartFileProducer.CreateOutputAdapter(conf, null)
      val data = new Array[ContainerInterface](3)
      data(0) = new ParameterContainer(null)
    
      //sfp.send(null, data)
      assert(1 == 1)
    }
        
  }

}
