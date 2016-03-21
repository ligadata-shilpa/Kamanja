package org.kamanja.serdeser.json

import org.json4s.jackson.JsonMethods._
import org.json4s.{MappingException, DefaultFormats, Formats}
import scala.reflect.runtime.{universe => ru}

import scala.collection.mutable.{ArrayBuffer }

import java.io.{DataInputStream, ByteArrayInputStream, DataOutputStream, ByteArrayOutputStream}

import org.apache.logging.log4j._
import com.fasterxml.jackson.databind.ObjectMapper

import com.ligadata.kamanja.metadata.MiningModelType
import com.ligadata.kamanja.metadata.ModelRepresentation
import com.ligadata.kamanja.metadata._
import com.ligadata.kamanja.metadata.ObjType._
import com.ligadata.kamanja.metadata.MdMgr._

import com.ligadata.Exceptions._
import com.ligadata.KamanjaBase._

import scala.reflect.runtime._

/**
  * Meta fields found at the beginning of each JSON representation of a ContainerInterface object
  */
object JsonContainerInterfaceKeys extends Enumeration {
    type JsonKeys = Value
    val typename, version, physicalname = Value
}


/**
  * JSONSerDeser instance can serialize a ContainerInterface to a byte array and deserialize a byte array to form
  * an instance of the ContainerInterface encoded in its bytes.
  *
  * @param mgr a MdMgr instance that contains the relevant types, messages, containers in it to perform serialize/
  *            deserialize operations.
  * @param objResolver is an object that can fabricate an empty ContainerInterface
  * @param classLoader is an object that can generally instantiate class instances that are in the classpath of the loader
  *
  */

class JSONSerDes(val mgr : MdMgr
                 , var objResolver : ObjectResolver
                 , var classLoader : java.lang.ClassLoader) extends SerializeDeserialize with LogTrait {

    /**
      * Serialize the supplied container to a byte array
      *
      * @param v a ContainerInterface (describes a standard kamanja container)
      */
    def serialize(v : ContainerInterface) : Array[Byte] = {
        val bos: ByteArrayOutputStream = new ByteArrayOutputStream(8 * 1024)
        val dos = new DataOutputStream(bos)

        val withComma : Boolean = true
        val withoutComma :Boolean = false
        val containerName : String = v.getFullTypeName
        val containerVersion :String = v.getTypeVersion
        val container : ContainerTypeDef = mgr.ActiveType(containerName).asInstanceOf[ContainerTypeDef]
        val className : String = container.PhysicalName

        val containerJsonHead : String = "{ "
        val containerJsonTail : String = " }"
        val containerNameJson : String = nameValueAsJson(JsonContainerInterfaceKeys.typename.toString, containerName, true, withComma)
        val containerVersionJson : String = nameValueAsJson(JsonContainerInterfaceKeys.version.toString, containerVersion,  true, withComma)
        val containerPhyNameJson : String = nameValueAsJson(JsonContainerInterfaceKeys.physicalname.toString, className,  true, withComma)

        dos.writeChars(containerJsonHead)
        dos.writeUTF(containerNameJson)
        dos.writeUTF(containerVersionJson)
        dos.writeUTF(containerPhyNameJson)

        val fields : Array[AttributeNameAndValue] = v.getAllAttributeValues
        val lastAttr : AttributeNameAndValue = fields.last
        fields.foreach(attr => {
            val name : String = attr.getAttributeName
            val attrTypeValue : AttributeValue = attr.getAttributeValue
            val typeName : String = attrTypeValue.getValueType
            val fieldTypeDef : BaseTypeDef = mgr.ActiveType(typeName)
            val rawValue : Any = attrTypeValue.getValue
            val useComma : Boolean = if (lastAttr != attr) withComma else withoutComma

            val isContainerType : Boolean = isContainerTypeDef(fieldTypeDef)
            val fldRep : String = if (isContainerType) {
                processContainerTypeAsJson(fieldTypeDef.asInstanceOf[ContainerTypeDef]
                                        , rawValue
                                        , useComma)
            } else {
                val quoteValue : Boolean = useQuotesOnValue(fieldTypeDef)
                nameValueAsJson(typeName, rawValue, quoteValue, useComma)
            }

            dos.writeBytes(fldRep)
        })
        dos.writeChars(containerJsonTail)

        val strRep : String = dos.toString
        logger.debug(s"attribute as JSON:\n$strRep")

        val byteArray : Array[Byte] = bos.toByteArray
        dos.close()
        bos.close()
        byteArray
    }

    /**
      * Process the supplied ContainerTypeDef.  There are essentially two paths through this method.
      * One path recognizes the container to be MappedMsgTypeDef or StructTypeDef (i.e., instanceOf[ContainerInterface]).
      * In that event there is recursion back to the serialize(v : ContainerInterface) method.
      *
      * The other path pertains to the Array and Map type Containers.  Those with simple scalar types (scalars, String, Boolean)
      * are handled locally.  Array elements that have ContainerTypeDef elements are iterated and recursion happens
      * for each element.  For maps, if either the key or value member type is a ContainerTypeDef, each is similarly
      * managed.
      *
      * Fixme: There is no reason that Stacks, Queues and other data types we support cannot be implemented here.  For
      * now just Array, ArrayBuffer, Map, and ImmutableMap
      *
      * @param aContainerType the ContainerTypeDef that describes the supplied instance.
      * @param rawValue the actual container instance
      * @param withComma when true, a ',' is appended to the ContainerTypeDef Json representation
      * @return a Json String representation for the supplied ContainterTypeDef
      */
    private def processContainerTypeAsJson(aContainerType : ContainerTypeDef
                                         , rawValue : Any
                                         , withComma : Boolean) : String = {

        /** ContainerInterface instance? */
        val isContainerInterface : Boolean = rawValue.isInstanceOf[ContainerInterface]
        val stringRep : String = if (isContainerInterface) {
            val containerBytes : Array[Byte] = serialize(rawValue.asInstanceOf[ContainerInterface])
            val containerStr : String = new String(containerBytes)
            containerStr
        } else { /** Check for collection that is currently supported */
            val strrep : String = aContainerType match {
                case a : ArrayTypeDef =>  {
                    val array : Array[Any] = rawValue.asInstanceOf[Array[Any]]
                    arrayAsJson(aContainerType, array)
                }
                case ab : ArrayBufTypeDef => {
                        val array : ArrayBuffer[Any] = rawValue.asInstanceOf[ArrayBuffer[Any]]
                        arrayAsJson(aContainerType, array)
                }
                case m : MapTypeDef => {
                    val map : scala.collection.immutable.Map[Any,Any] = rawValue.asInstanceOf[scala.collection.immutable.Map[Any,Any]]
                    mapAsJson(aContainerType, map)
                }
                case im : ImmutableMapTypeDef =>  {
                    val map : scala.collection.mutable.Map[Any,Any] = rawValue.asInstanceOf[scala.collection.mutable.Map[Any,Any]]
                    mapAsJson(aContainerType, map)
                }
                case _ => throw new UnsupportedObjectException(s"container type ${aContainerType.typeString} not currently serializable",null)
            }

            strrep
        }
        stringRep
    }

   private def isContainerTypeDef(aType : BaseTypeDef) : Boolean = {
        aType.isInstanceOf[ContainerTypeDef]
   }

    private def nameValueAsJson(name : String, value : Any, quoteValue : Boolean, withComma : Boolean) : String = {
        val comma : String = if (withComma) "," else ""
        val quote : String = if (quoteValue) s"\\${'"'}" else ""
        s" {'\'}${'"'}$name{'\'}${'"'} : $quote$value$quote$comma"
    }

    private def useQuotesOnValue(fieldTypeDef : BaseTypeDef) : Boolean = {
        val usequotes : Boolean = if (fieldTypeDef == null) {
            true
        } else {
            /** This does not account for user defined versions of the scalars */
            fieldTypeDef.Name.toLowerCase match {
                case "int" | "integer" | "short" | "long" | "float" | "double" => false
                case _ => true
            }
        }
        usequotes
    }

    /**
      * Create a Json string from the supplied immutable map.  Note that either/both map elements can in turn be
      * containers.
      *
      * @param aContainerType The container type def for the supplied map
      * @param map the map instance
      * @return a Json string representation
      */
    private def mapAsJson(aContainerType : ContainerTypeDef, map : scala.collection.immutable.Map[Any,Any]) : String = {
        val memberTypes : Array[BaseTypeDef] = aContainerType.asInstanceOf[ImmutableMapTypeDef].ElementTypes
        val keyType : BaseTypeDef = memberTypes.head
        val valType : BaseTypeDef = memberTypes.last
        val itemAtATime : Boolean = (keyType.isInstanceOf[ContainerTypeDef] || valType.isInstanceOf[ContainerTypeDef])
        val mapAsJsonStr : String = if (itemAtATime) {
            val buffer : StringBuilder = new StringBuilder
            val lastVal : Any  = map.values.last
            val noComma : Boolean = false
            map.foreach(pair => {
                val keyRep : String =  if (keyType.isInstanceOf[ContainerTypeDef]) {
                    processContainerTypeAsJson(keyType.asInstanceOf[ContainerTypeDef], pair._1, noComma)
                } else {
                    val quoteValue : Boolean = true
                    nameValueAsJson(keyType.FullName, pair._1, quoteValue, noComma)
                }
                val printComma : Boolean = lastVal != pair._2
                val valRep : String =  if (valType.isInstanceOf[ContainerTypeDef]) {
                    processContainerTypeAsJson(valType.asInstanceOf[ContainerTypeDef], pair._2, printComma)
                } else {
                    val quoteValue : Boolean = useQuotesOnValue(valType)
                    nameValueAsJson(valType.FullName, pair._2, quoteValue, printComma)
                }
                buffer.append(s"$keyRep : $valRep")
            })
            buffer.toString
        } else {
            new ObjectMapper().writeValueAsString(map)
        }
        mapAsJsonStr
    }

    /**
      * Create a Json string from the supplied mutable map.  Note that either/both map elements can in turn be
      * containers.
      *
      * @param aContainerType The container type def for the supplied map
      * @param map the map instance
      * @return a Json string representation
      */
    private def mapAsJson(aContainerType : ContainerTypeDef, map : scala.collection.mutable.Map[Any,Any]) : String = {
        val memberTypes : Array[BaseTypeDef] = aContainerType.asInstanceOf[ImmutableMapTypeDef].ElementTypes
        val keyType : BaseTypeDef = memberTypes.head
        val valType : BaseTypeDef = memberTypes.last
        val itemAtATime : Boolean = (keyType.isInstanceOf[ContainerTypeDef] || valType.isInstanceOf[ContainerTypeDef])
        val mapAsJsonStr : String = if (itemAtATime) {
            val buffer : StringBuilder = new StringBuilder
            val lastVal : Any  = map.values.last
            val noComma : Boolean = false
            buffer.append("{ ")
            map.foreach(pair => {
                val keyRep : String =  if (keyType.isInstanceOf[ContainerTypeDef]) {
                    processContainerTypeAsJson(keyType.asInstanceOf[ContainerTypeDef], pair._1, noComma)
                } else {
                    val quoteValue : Boolean = true
                    nameValueAsJson(keyType.FullName, pair._1, quoteValue, noComma)
                }
                val printComma : Boolean = lastVal != pair._2
                val valRep : String =  if (valType.isInstanceOf[ContainerTypeDef]) {
                    processContainerTypeAsJson(valType.asInstanceOf[ContainerTypeDef], pair._2, printComma)
                } else {
                    val quoteValue : Boolean = useQuotesOnValue(valType)
                    nameValueAsJson(valType.FullName, pair._2, quoteValue, printComma)
                 }
                buffer.append(s"$keyRep : $valRep")
            })
            buffer.append(" }")
            buffer.toString
        } else {
            new ObjectMapper().writeValueAsString(map)
        }
        mapAsJsonStr
    }

    /**
      * Create a Json string from the supplied array.  Note that the array elements can themselves
      * be containers.
      *
      * @param aContainerType The container type def for the supplied array
      * @param array the array instance
      * @return a Json string representation
      */
    private def arrayAsJson(aContainerType : ContainerTypeDef, array : Array[Any]) : String = {
        val memberTypes : Array[BaseTypeDef] = aContainerType.asInstanceOf[ImmutableMapTypeDef].ElementTypes
        val itmType : BaseTypeDef = memberTypes.last
        val itemAtATime : Boolean = itmType.isInstanceOf[ContainerTypeDef]
        val arrAsJsonStr : String = if (itemAtATime) {
            val buffer : StringBuilder = new StringBuilder
            val lastItm : Any  = array.last
            buffer.append("[ ")
            array.foreach(itm => {
                val printComma : Boolean = lastItm != itm
                val itmRep : String =  if (itmType.isInstanceOf[ContainerTypeDef]) {
                    processContainerTypeAsJson(itmType.asInstanceOf[ContainerTypeDef], itm, printComma)
                } else {
                    val quoteValue : Boolean = useQuotesOnValue(itmType)
                    nameValueAsJson(itmType.FullName, itm, quoteValue, printComma)
                }
                buffer.append(s"$itm")
            })
            buffer.append(" ]")
            buffer.toString
        } else {
            new ObjectMapper().writeValueAsString(array)
        }
        arrAsJsonStr
    }

    /**
      * Create a Json string from the supplied array buffer.  Note that the array buffer elements can themselves
      * be containers.
      *
      * @param aContainerType The container type def for the supplied array buffer
      * @param array the array instance
      * @return a Json string representation
      */
    private def arrayAsJson(aContainerType : ContainerTypeDef, array : ArrayBuffer[Any]) : String = {
        val memberTypes : Array[BaseTypeDef] = aContainerType.asInstanceOf[ImmutableMapTypeDef].ElementTypes
        val itmType : BaseTypeDef = memberTypes.last
        val itemAtATime : Boolean = itmType.isInstanceOf[ContainerTypeDef]
        val arrAsJsonStr : String = if (itemAtATime) {
            val buffer : StringBuilder = new StringBuilder
            val lastItm : Any  = array.last
            buffer.append("[ ")
            array.foreach(itm => {
                val printComma : Boolean = lastItm != itm
                val itmRep : String =  if (itmType.isInstanceOf[ContainerTypeDef]) {
                    processContainerTypeAsJson(itmType.asInstanceOf[ContainerTypeDef], itm, printComma)
                } else {
                    val quoteValue : Boolean = useQuotesOnValue(itmType)
                    nameValueAsJson(itmType.FullName, itm, quoteValue, printComma)
                }
                buffer.append(s"$itm")
            })
            buffer.append(" ]")
            buffer.toString
        } else {
            new ObjectMapper().writeValueAsString(array)
        }
        arrAsJsonStr
    }

    /**
      * Set the object resolver to be used for this serializer
 *
      * @param objRes an ObjectResolver
      */
    def setObjectResolver(objRes : ObjectResolver) : Unit = {
        objResolver = objRes;
    }

    /**
      * Deserialize the supplied byte array into some ContainerInterface instance.
      *
      * @param b the byte array containing the serialized ContainerInterface instance
      * @return a ContainerInterface
      */
    def deserialize(b: Array[Byte]) : ContainerInterface = {

        val rawJsonContainerStr : String = new String(b)
        val containerInstanceMap : Map[String, Any] = jsonStringAsMap(rawJsonContainerStr)

        /** Decode the map to produce an instance of ContainerInterface */

        /** get the container key information.. the top level object must be a ContainerInterface... if these
          * are not present, nothing good will come of it */
        val containerNameJson : String = containerInstanceMap.getOrElse(JsonContainerInterfaceKeys.typename.toString, "").asInstanceOf[String]
        val containerVersionJson : String = containerInstanceMap.getOrElse(JsonContainerInterfaceKeys.version.toString, "").asInstanceOf[String]
        val containerPhyNameJson : String = containerInstanceMap.getOrElse(JsonContainerInterfaceKeys.physicalname.toString, "").asInstanceOf[String]

        if (containerNameJson.size == 0) {
            throw new MissingPropertyException("the supplied byte array to deserialize does not have a container name.", null)
        }

        /** get an empty ContainerInterface instance for this type name from the objResolver */
        val ci : ContainerInterface = objResolver.getInstance(classLoader, containerNameJson)
        if (ci == null) {
            throw new ObjectNotFoundException(s"type name $containerNameJson could not be resolved and built for deserialize",null)
        }

        /** get the fields information */
        val containerBaseType : BaseTypeDef = mgr.ActiveType(containerNameJson)
        val containerType : ContainerTypeDef = if (containerBaseType != null) containerBaseType.asInstanceOf[ContainerTypeDef] else null
        if (containerType == null) {
            throw new ObjectNotFoundException(s"type name $containerNameJson is not a container type... deserialize fails.",null)
        }
        val fieldTypes : Array[BaseTypeDef] = containerType.ElementTypes
        fieldTypes.foreach(fieldType => {

            val fieldsJson : Any = containerInstanceMap.getOrElse(fieldType.FullName, null)
            val isContainerType : Boolean = isContainerTypeDef(fieldType)
            val fld : Any = if (isContainerType) {
                val containerTypeInfo : ContainerTypeDef = fieldType.asInstanceOf[ContainerTypeDef]
                createContainerType(containerTypeInfo, fieldsJson)
            } else {
                /** currently assumed to be one of the scalars or simple types supported by json/avro */
                fieldsJson
            }
            ci.set(fieldType.FullName, fld)
        })

        val container : ContainerInterface = null
        container
    }

    def createContainerType(containerTypeInfo : ContainerTypeDef, fieldsJson : Any) : Any = {
        /** ContainerInterface instance? */
        val isContainerInterface : Boolean = containerTypeInfo.isInstanceOf[MappedMsgTypeDef] || containerTypeInfo.isInstanceOf[StructTypeDef]
        val containerInst : Any = if (isContainerInterface) {
            /** recurse to obtain the subcontainer */
            val containerBytes : Array[Byte] = fieldsJson.toString.toCharArray.map(_.toByte)
            val container : ContainerInterface = deserialize(containerBytes)
            container
        } else { /** Check for collection that is currently supported */

            val coll : Any = containerTypeInfo match {
                case a : ArrayTypeDef =>  {
                    val collElements : List[Map[String, Any]] = fieldsJson.asInstanceOf[List[Map[String, Any]]]
                    jsonAsArray(containerTypeInfo, collElements)
                }
                case ab : ArrayBufTypeDef => {
                    val collElements : List[Map[String, Any]] = fieldsJson.asInstanceOf[List[Map[String, Any]]]
                    jsonAsArrayBuffer(containerTypeInfo, collElements)
                }
                case m : MapTypeDef => {
                    val collElements : Map[String, Any] = fieldsJson.asInstanceOf[Map[String, Any]]
                    jsonAsMap(containerTypeInfo, collElements)
                }
                case im : ImmutableMapTypeDef =>  {
                    val collElements : Map[String, Any] = fieldsJson.asInstanceOf[Map[String, Any]]
                    jsonAsMutableMap(containerTypeInfo, collElements)
                }
                case _ => throw new UnsupportedObjectException(s"container type ${containerTypeInfo.typeString} not currently serializable",null)
            }

            coll
        }
        containerInst
    }

    /**
      * Coerce the list of mapped elements to an array of the mapped elements' values
      *
      * @param arrayTypeInfo the metadata that describes the array
      * @param collElements the list of json elements for the array buffer
      * @return an array instance
      */
    def jsonAsArray(arrayTypeInfo : ContainerTypeDef, collElements : List[Map[String,Any]]) : Array[Any] = {

        /**
         * FIXME: if we intend to support arrays of hetergeneous items (i.e, Array[Any]), this has to change.  At the
         * moment only arrays of homogeneous types are supported.
         */

        val memberTypes : Array[BaseTypeDef] = arrayTypeInfo.asInstanceOf[ImmutableMapTypeDef].ElementTypes
        val itmType : BaseTypeDef = memberTypes.last
        val arrayMbrTypeIsContainer : Boolean = itmType.isInstanceOf[ContainerTypeDef]
        val array : Array[Any] = if (collElements.size > 0) {
            val list : List[Any] = collElements.map(itm => {
                if (arrayMbrTypeIsContainer) {
                    val itmType : ContainerTypeDef = itm.asInstanceOf[ContainerTypeDef]
                    createContainerType(itmType, itm)
                } else {
                    itm
                }
            })
            list.toArray
        } else {
            Array[Any]()
        }
        array
    }

    /**
      * Coerce the list of mapped elements to an array buffer of the mapped elements' values
      *
      * @param arrayTypeInfo the metadata that describes the array buffer
      * @param collElements the list of json elements for the array buffer
      * @return an array buffer instance
      */
    def jsonAsArrayBuffer(arrayTypeInfo : ContainerTypeDef, collElements : List[Map[String,Any]]) : ArrayBuffer[Any] = {

        /**
         * FIXME: if we intend to support arrays of hetergeneous items (i.e, Array[Any]), this has to change.  At the
         * moment only arrays of homogeneous types are supported.
         */

        val memberTypes : Array[BaseTypeDef] = arrayTypeInfo.asInstanceOf[ImmutableMapTypeDef].ElementTypes
        val itmType : BaseTypeDef = memberTypes.last
        val arrayMbrTypeIsContainer : Boolean = itmType.isInstanceOf[ContainerTypeDef]
        val arraybuffer : ArrayBuffer[Any] = ArrayBuffer[Any]()
        collElements.foreach(itm => {
            if (arrayMbrTypeIsContainer) {
                val itmType : ContainerTypeDef = itm.asInstanceOf[ContainerTypeDef]
                val arrbItm : Any = createContainerType(itmType, itm)
                arraybuffer += arrbItm
            } else {
                arraybuffer += itm
            }
        })
        arraybuffer

    }

    /**
      * Coerce the list of mapped elements to an immutable map of the mapped elements' values
      *
      * @param mapTypeInfo
      * @param collElements
      * @return
      */
    def jsonAsMap(mapTypeInfo : ContainerTypeDef, collElements : Map[String,Any]) : scala.collection.immutable.Map[Any,Any] = {
        val memberTypes : Array[BaseTypeDef] = mapTypeInfo.asInstanceOf[ImmutableMapTypeDef].ElementTypes
        val sanityChk : Boolean = memberTypes.length == 2
        val keyType : BaseTypeDef = memberTypes.head
        val valType : BaseTypeDef = memberTypes.last
        val map : scala.collection.immutable.Map[Any,Any] = collElements.map(pair => {
            val key : String = pair._1
            val value : Any = pair._2
            val keyRep : Any =  if (keyType.isInstanceOf[ContainerTypeDef]) {
                val itmType : ContainerTypeDef = key.asInstanceOf[ContainerTypeDef]
                createContainerType(itmType, key)
            } else {
                key
            }

            val valRep : Any =  if (valType.isInstanceOf[ContainerTypeDef]) {
                val itmType : ContainerTypeDef = value.asInstanceOf[ContainerTypeDef]
                createContainerType(itmType, value)
            } else {
                value
            }
            (keyRep,valRep)
        }).toMap

        map
    }

    /**
      * Coerce the list of mapped elements to an mutable map of the mapped elements' values
      *
      * @param mapTypeInfo
      * @param collElements
      * @return
      */
    def jsonAsMutableMap(mapTypeInfo : ContainerTypeDef, collElements : Map[String,Any]) : scala.collection.mutable.Map[Any,Any] = {
        val memberTypes : Array[BaseTypeDef] = mapTypeInfo.asInstanceOf[ImmutableMapTypeDef].ElementTypes
        val sanityChk : Boolean = memberTypes.length == 2
        val keyType : BaseTypeDef = memberTypes.head
        val valType : BaseTypeDef = memberTypes.last
        val map : scala.collection.mutable.Map[Any,Any] = scala.collection.mutable.Map[Any,Any]()
        collElements.foreach(pair => {
            val key : String = pair._1
            val value : Any = pair._2
            val keyRep : Any =  if (keyType.isInstanceOf[ContainerTypeDef]) {
                val itmType : ContainerTypeDef = key.asInstanceOf[ContainerTypeDef]
                createContainerType(itmType, key)
            } else {
                key
            }

            val valRep : Any =  if (valType.isInstanceOf[ContainerTypeDef]) {
                val itmType : ContainerTypeDef = value.asInstanceOf[ContainerTypeDef]
                createContainerType(itmType, value)
            } else {
                value
            }
            map(keyRep) = valRep
        })

        map
    }

    /**
      * Translate the supplied json string to a Map[String, Any]
      *
      * @param configJson
      * @return Map[String, Any]
      */

    @throws(classOf[com.ligadata.Exceptions.Json4sParsingException])
    @throws(classOf[com.ligadata.Exceptions.EngineConfigParsingException])
    def jsonStringAsMap(configJson: String): Map[String, Any] = {
        try {
            implicit val jsonFormats: Formats = DefaultFormats
            val json = parse(configJson)
            logger.debug("Parsed the json : " + configJson)

            val fullmap = json.values.asInstanceOf[Map[String, Any]]

            fullmap
        } catch {
            case e: MappingException => {
                logger.debug("", e)
                throw Json4sParsingException(e.getMessage, e)
            }
            case e: Exception => {
                logger.debug("", e)
                throw EngineConfigParsingException(e.getMessage, e)
            }
        }
    }



}

