package org.kamanja.serdeser.json

import java.io.{DataOutputStream, ByteArrayOutputStream}

import com.fasterxml.jackson.databind.ObjectMapper
import com.ligadata.kamanja.metadata.MiningModelType
import com.ligadata.kamanja.metadata.ModelRepresentation
import com.ligadata.kamanja.metadata._
import com.ligadata.kamanja.metadata.ObjType._
import com.ligadata.kamanja.metadata.MdMgr._
import scala.collection.mutable
import scala.collection.mutable.{ ArrayBuffer }
import org.apache.logging.log4j._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import com.ligadata.Exceptions._
import com.ligadata.KamanjaBase._

/**
  *
  *
  * static String getEmployeeJson() {
  * return "{                                 " +
  * "    \"id\" : 1001,                 " +
  * "    \"name\" : \"Drona\",          " +
  * "    \"age\" : 25,                  " +
  * "    \"designation\" : \"Manager\", " +
  * "    \"compensation\" : {           " +
  * "        \"currency\" : \"₹\",      " +
  * "        \"salary\" : 30000         " +
  * "    }                              " +
  * "}                                 ";
  * }
  *
  * val typName = dis.readUTF
  * val version = dis.readUTF
  * val classname = dis.readUTF

  *
  */

class JSONSerDes(val mgr : MdMgr) extends SerializeDeserialize {
    var _objResolver : ObjectResolver = null
    var _loader : java.lang.ClassLoader = null

    /**
      * Serialize the supplied container to a byte array
      *
      * @param v a ContainerInterface (describes a standard kamanja container
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
        val containerNameJson : String = nameValueAsJson("typename", containerName, withComma)
        val containerVersionJson : String = nameValueAsJson("version", containerVersion, withComma)
        val containerPhyNameJson : String = nameValueAsJson("physicalname", className, withComma)

        dos.writeUTF(containerNameJson)
        dos.writeUTF(containerVersionJson)
        dos.writeUTF(containerPhyNameJson)

        val fields : Array[AttributeNameAndValue] = v.getAllAttributeValues
        val lastAttr : AttributeNameAndValue = fields.last
        dos.writeChars(containerJsonHead)
        fields.foreach(attr => {
            val name : String = attr.getAttributeName
            val attrTypeValue : AttributeValue = attr.getAttributeValue
            val typeName : String = attrTypeValue.getValueType
            val fieldTypeDef : BaseTypeDef = mgr.ActiveType(typeName)
            val rawValue : AnyRef = attrTypeValue.getValue

            val isContainerType : Boolean = isContainerTypeDef(fieldTypeDef)
            val fldRep : String = if (isContainerType) {
                processContainerTypeAsJson(fieldTypeDef.asInstanceOf[ContainerTypeDef]
                                        , rawValue
                                        , if (lastAttr != attr) withComma else withoutComma)


            } else {
                nameValueAsJson(typeName, rawValue, if (lastAttr != attr) withComma else withoutComma)
            }

            dos.writeBytes(fldRep)
        })
        dos.writeChars(containerJsonTail)

        val returnVal : Array[Byte] = bos.toByteArray
        dos.close
        bos.close
        returnVal
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
                                         , rawValue : AnyRef
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
                    val array : Array[AnyRef] = rawValue.asInstanceOf[Array[AnyRef]]
                    arrayAsJson(aContainerType, array)
                }
                case ab : ArrayBufTypeDef => {
                        val array : ArrayBuffer[AnyRef] = rawValue.asInstanceOf[ArrayBuffer[AnyRef]]
                        arrayAsJson(aContainerType, array)
                }
                case m : MapTypeDef => {
                    val map : scala.collection.immutable.Map[AnyRef,AnyRef] = rawValue.asInstanceOf[scala.collection.immutable.Map[AnyRef,AnyRef]]
                    mapAsJson(aContainerType, map)
                }
                case im : ImmutableMapTypeDef =>  {
                    val map : scala.collection.mutable.Map[AnyRef,AnyRef] = rawValue.asInstanceOf[scala.collection.mutable.Map[AnyRef,AnyRef]]
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

    private def nameValueAsJson(name : String, value : AnyRef, withComma : Boolean) : String = {
        val comma : String = if (withComma) "," else ""
        s" {'\'}${'"'}$name{'\'}${'"'} : {'\'}${'"'}$value{'\'}${'"'}$comma"
    }

    /**
      * Create a Json string from the supplied immutable map.  Note that either/both map elements can in turn be
      * containers.
      *
      * @param aContainerType The container type def for the supplied map
      * @param map the map instance
      * @return a Json string representation
      */
    private def mapAsJson(aContainerType : ContainerTypeDef, map : scala.collection.immutable.Map[AnyRef,AnyRef]) : String = {
        val memberTypes : Array[BaseTypeDef] = aContainerType.asInstanceOf[ImmutableMapTypeDef].ElementTypes
        val keyType : BaseTypeDef = memberTypes.head
        val valType : BaseTypeDef = memberTypes.last
        val itemAtATime : Boolean = (keyType.isInstanceOf[ContainerTypeDef] || valType.isInstanceOf[ContainerTypeDef])
        val mapAsJsonStr : String = if (itemAtATime) {
            val buffer : StringBuilder = new StringBuilder
            val lastVal : AnyRef  = map.values.last
            val noComma : Boolean = false
            map.foreach(pair => {
                val keyRep : String =  if (keyType.isInstanceOf[ContainerTypeDef]) {
                    processContainerTypeAsJson(keyType.asInstanceOf[ContainerTypeDef], pair._1, noComma)
                } else {
                    nameValueAsJson(keyType.FullName, pair._1, noComma)
                }
                val printComma : Boolean = lastVal != pair._2
                val valRep : String =  if (valType.isInstanceOf[ContainerTypeDef]) {
                    processContainerTypeAsJson(valType.asInstanceOf[ContainerTypeDef], pair._2, printComma)
                } else {
                    nameValueAsJson(valType.FullName, pair._2, printComma)
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
    private def mapAsJson(aContainerType : ContainerTypeDef, map : scala.collection.mutable.Map[AnyRef,AnyRef]) : String = {
        val memberTypes : Array[BaseTypeDef] = aContainerType.asInstanceOf[ImmutableMapTypeDef].ElementTypes
        val keyType : BaseTypeDef = memberTypes.head
        val valType : BaseTypeDef = memberTypes.last
        val itemAtATime : Boolean = (keyType.isInstanceOf[ContainerTypeDef] || valType.isInstanceOf[ContainerTypeDef])
        val mapAsJsonStr : String = if (itemAtATime) {
            val buffer : StringBuilder = new StringBuilder
            val lastVal : AnyRef  = map.values.last
            val noComma : Boolean = false
            buffer.append("{ ")
            map.foreach(pair => {
                val keyRep : String =  if (keyType.isInstanceOf[ContainerTypeDef]) {
                    processContainerTypeAsJson(keyType.asInstanceOf[ContainerTypeDef], pair._1, noComma)
                } else {
                    nameValueAsJson(keyType.FullName, pair._1, noComma)
                }
                val printComma : Boolean = lastVal != pair._2
                val valRep : String =  if (valType.isInstanceOf[ContainerTypeDef]) {
                    processContainerTypeAsJson(valType.asInstanceOf[ContainerTypeDef], pair._2, printComma)
                } else {
                    nameValueAsJson(valType.FullName, pair._2, printComma)
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
    private def arrayAsJson(aContainerType : ContainerTypeDef, array : Array[AnyRef]) : String = {
        val memberTypes : Array[BaseTypeDef] = aContainerType.asInstanceOf[ImmutableMapTypeDef].ElementTypes
        val itmType : BaseTypeDef = memberTypes.last
        val itemAtATime : Boolean = itmType.isInstanceOf[ContainerTypeDef]
        val arrAsJsonStr : String = if (itemAtATime) {
            val buffer : StringBuilder = new StringBuilder
            val lastItm : AnyRef  = array.last
            buffer.append("[ ")
            array.foreach(itm => {
                val printComma : Boolean = lastItm != itm
                val itmRep : String =  if (itmType.isInstanceOf[ContainerTypeDef]) {
                    processContainerTypeAsJson(itmType.asInstanceOf[ContainerTypeDef], itm, printComma)
                } else {
                    nameValueAsJson(itmType.FullName, itm, printComma)
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
    private def arrayAsJson(aContainerType : ContainerTypeDef, array : ArrayBuffer[AnyRef]) : String = {
        val memberTypes : Array[BaseTypeDef] = aContainerType.asInstanceOf[ImmutableMapTypeDef].ElementTypes
        val itmType : BaseTypeDef = memberTypes.last
        val itemAtATime : Boolean = itmType.isInstanceOf[ContainerTypeDef]
        val arrAsJsonStr : String = if (itemAtATime) {
            val buffer : StringBuilder = new StringBuilder
            val lastItm : AnyRef  = array.last
            buffer.append("[ ")
            array.foreach(itm => {
                val printComma : Boolean = lastItm != itm
                val itmRep : String =  if (itmType.isInstanceOf[ContainerTypeDef]) {
                    processContainerTypeAsJson(itmType.asInstanceOf[ContainerTypeDef], itm, printComma)
                } else {
                    nameValueAsJson(itmType.FullName, itm, printComma)
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
      * @param objRes an ObjectResolver
      */
    def setObjectResolver(objRes : ObjectResolver) : Unit = {
        _objResolver = objRes;
    }

    /**
      * Deserialize the supplied byte array into some ContainerInterface instance.
      *
      * @param b the byte array containing the serialized ContainerInterface instance
      * @return a ContainerInterface
      */
    def deserialize(b: Array[Byte]) : ContainerInterface = {
        /** Fixme: */
        val container : ContainerInterface = null
        container
    }
}

