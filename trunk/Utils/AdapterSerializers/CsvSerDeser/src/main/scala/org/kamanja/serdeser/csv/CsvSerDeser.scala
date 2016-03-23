package org.kamanja.serdeser.csv


import scala.collection.mutable.{ArrayBuffer }
import scala.collection.JavaConverters._

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
object CsvContainerInterfaceKeys extends Enumeration {
    type CsvKeys = Value
    val typename, version, physicalname = Value
}


/**
  * CsvSerDeser instance can serialize a ContainerInterface to a byte array and deserialize a byte array to form
  * an instance of the ContainerInterface encoded in its bytes.
  *
  * @param mgr a MdMgr instance that contains the relevant types, messages, containers in it to perform serialize/
  *            deserialize operations.
  * @param serDeserConfig the SerializeDeserializeConfig that describes/generally configures this instance
  * @param emitHeaderFirst used usually on first message to be emitted (if at all), the header names are emitted when true
  * @param objResolver is an object that can fabricate an empty ContainerInterface
  * @param classLoader is an object that can generally instantiate class instances that are in the classpath of the loader
  *
  */

class CsvSerDeser(val mgr : MdMgr
                 , val serDeserConfig : SerializeDeserializeConfig
                 , var emitHeaderFirst : Boolean
                 , var objResolver : ObjectResolver
                 , var classLoader : java.lang.ClassLoader) extends SerializeDeserialize with LogTrait {

    /**
      * Serialize the supplied container to a byte array using these CSV rules:
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
        val containerFieldsInOrder : Array[BaseTypeDef] = container.ElementTypes

        val fields : java.util.HashMap[String,com.ligadata.KamanjaBase.AttributeValue] = v.getAllAttributeValues
        if (emitHeaderFirst) {
            emitHeaderRecord(dos, containerFieldsInOrder)
        }


        /** write the first field */
        val containerNameCsv : String = csvTypeInfo(CsvContainerInterfaceKeys.typename.toString, serDeserConfig.fieldDelimiter)
        dos.writeUTF(containerNameCsv)

        //val containerVersionCsv : String = nameValueAsJson(CsvContainerInterfaceKeys.version.toString, containerVersion,  true, withComma)
        //val containerPhyNameCsv : String = nameValueAsJson(CsvContainerInterfaceKeys.physicalname.toString, className,  true, withComma)

        var processCnt : Int = 0
        val fieldCnt : Int = fields.size()
        containerFieldsInOrder.foreach(typedef => {

            processCnt += 1

            val attr : com.ligadata.KamanjaBase.AttributeValue = fields.getOrDefault(typedef.FullName, null)
            if (attr != null) {
                val doTrailingComma : Boolean = processCnt < fieldCnt
                emitField(dos, typedef, attr, doTrailingComma)
            } else {
                throw new ObjectNotFoundException(s"during serialize()...attribute ${typedef.FullName} could not be found in the container... mismatch", null)
            }
        })

        val strRep : String = dos.toString
        logger.debug(s"attribute as JSON:\n$strRep")

        val byteArray : Array[Byte] = bos.toByteArray
        dos.close()
        bos.close()
        byteArray
    }

    /**
      * Format one of the type info fields by quoting the supplied value and tagging the delimiter in use to it.
      * @param value
      * @param fldDelim
      * @return decorated string for emission
      */
    private def csvTypeInfo(value : String, fldDelim : String) : String = {
        val quote : String = s"${'"'}"
        s"$quote{'\'}$value$quote$fldDelim"
    }


    /**
      * Write the field data type names to the supplied stream.  This is called whenever the emitHeaderFirst instance
      * variable is true... usually just once in a given stream creation.
      * @param dos
      * @param containerFieldsInOrder
      */
    private def emitHeaderRecord(dos : DataOutputStream, containerFieldsInOrder : Array[BaseTypeDef]) = {
        val quote : String = s"${'"'}"
        val fieldCnt : Int = containerFieldsInOrder.length
        var cnt : Int = 0
        containerFieldsInOrder.foreach(typedef => {
            cnt += 1
            val delim : String = if (cnt < fieldCnt) serDeserConfig.fieldDelimiter else ""
            val value : String = s"$quote${typedef.FullName}$quote"
            dos.writeUTF(s"$value$delim")
        })
    }

    /**
      * Emit the supplied attribute's data value, possibly decorating it with commas or newline, escaped quotes, etc.
      *
      * CSV rules supported:
      * 1) CSV is a delimited data format that has fields/columns separated by the comma character and records/rows terminated by newlines.
      * 2) A CSV file does not require a specific character encoding, byte order, or line terminator format (some software does not support all line-end variations).
      * 3) A record ends at a line terminator. However, line-terminators can be embedded as data within fields.  For fields with embedded line terminators
      * the field will be automatically enclosed in double quotes
      * 4) All records should have the same number of fields, in the same order.
      * 5) Numeric quantities are expressed in decimal.  Perhaps we will support other text representations (e.g., hex) at a later time.
      * 6) Field delimiters can be any char but usually {,;:\t}
      * 7) A field with embedded field delimiters must be enclosed in double quotes.
      * 8) The first record may be a "header", which contains column names in each of the fields. A header record's presence is
      * specified in the CSV serializer metadata. Once a file or stream has been produced with the CSV serializer, there is no
      * identifying markings in the file that there is a header.  Apriori, one most know.
      * 9) Fields with embedded double quote marks are quoted and each occurrence of a double quote mark is escaped with a double quote mark.
      * For example, a field value
      *     This is not a "joke"
      * will be encoded as
      *     "This is not a ""joke"""
      * 10) A CSV serializer may be configured to always quote a field.  If not so configured, only fields that require them (embedded new
      * lines, quote marks)
      * 11) Record (line) delimiters can be configured.  By default, the delimiter is the MS-DOS delimiter (\r\n).  The typical *nix line
      * (\n) is also supported.
      *
      * @param dos the data output stream to receive the emissions of the decorated value
      * @param typedef a data type metadata from the ContainerInterface's ContainerTypeDef ElementTypes
      * @param attr the attribute value that contains the data value
      * @param doTrailingComma when true follow data emission with comma; when false, emit the line delimiter configured
      */
    private def emitField( dos : DataOutputStream
                          ,typedef : BaseTypeDef
                          ,attr : com.ligadata.KamanjaBase.AttributeValue
                          ,doTrailingComma : Boolean) = {
        val valueType : String = attr.getValueType
        val rawValue : Any = attr.getValue
        val typeName : String = typedef.FullName
        logger.debug(s"emit field $typeName with original value = ${rawValue.toString}")

        val valueStr : String = attr.getValue.toString

        /** What if anything must be done about the delimiters appearing in the string? */
        /** Rule 3 */
        val containsNewLines : Boolean = valueStr != null &&
            (valueStr.indexOf('\n') >= 0 || valueStr.indexOf('\r') >= 0)
        /** Rule 7 */
        val hasFieldDelims : Boolean = valueStr.contains(serDeserConfig.fieldDelimiter)
        /** Rule 9 */
        val containsQuotes : Boolean = valueStr != null && valueStr.contains(s"${'"'}")

        val enclosingDblQuote : String = if (containsNewLines || hasFieldDelims || containsQuotes || serDeserConfig.alwaysQuoteField) s"${'"'}" else ""

        /** Rule 9 escape the embedded quotes */
        val valueStrAdjusted : String = if (containsQuotes) {
            s"$enclosingDblQuote${escapeQuotes(valueStr)}$enclosingDblQuote"
        } else {
            s"$enclosingDblQuote$valueStr$enclosingDblQuote"
        }
        logger.debug(s"emit field $typeName with value possibly quoted and escaped = $valueStrAdjusted")
        dos.writeUTF(valueStrAdjusted)
    }

    /**
      * Escape all double quotes found in this string by prefixing the double quote with another double quote.  If
      * none are found, the original string is returned.
      * @param valueStr string presumably that has embedded double quotes.
      * @return adjusted string
      */
    private def escapeQuotes(valueStr : String) : String = {
        val len : Int = if (valueStr != null) valueStr.length else 0
        val buffer : StringBuilder = new StringBuilder
        var base : Int = 0
        var startPoint : Int = 0
        while (startPoint >= 0) {
            startPoint = valueStr.indexOf('"', startPoint)
            if (startPoint >= 0) {
                val aSlice : String = valueStr.slice(base,startPoint)
                buffer.append(aSlice)
                buffer.append(s"${'"'}${'"'}")
                startPoint += 1 // start after the processed quote
                base = startPoint
                if (startPoint == len) {
                    startPoint = -1 // end it ... avoid out of bounds
                }
            }
        }
        val escapeQuotedStr : String = if (buffer.length == 0) valueStr else buffer.toString
        escapeQuotedStr
    }

    /**
      *
      * serDeserConfig
    class SerializeDeserializeConfig ( var serDeserType : SerializeDeserializeType.SerDeserType
                                 , var jar : String
                                 , var lineDelimiter : String = "\r\n"
                                 , var fieldDelimiter : String = ","
                                 , var produceHeader : Boolean = false
                                 , var alwaysQuoteField : Boolean = false) extends BaseElemDef {}
      */



    private def isContainerTypeDef(aType : BaseTypeDef) : Boolean = {
        aType.isInstanceOf[ContainerTypeDef]
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
      * Deserialize the supplied byte array into some ContainerInterface instance.  Note that the header
      * record is not handled.  The CSV stream of multiple container interface records will just stumble over
      * such header records when they don't match the
      *
      * @param b the byte array containing the serialized ContainerInterface instance
      * @return a ContainerInterface
      */
    def deserialize(b: Array[Byte]) : ContainerInterface = {

        val rawCsvContainerStr : String = new String(b)
        val containerInstanceMap : Map[String, Any] = csvStringAsMap(rawCsvContainerStr)

        /** Decode the map to produce an instance of ContainerInterface */

        /** get the container key information.. the top level object must be a ContainerInterface... if these
          * are not present, nothing good will come of it */
        val containerNameCsv : String = containerInstanceMap.getOrElse(CsvContainerInterfaceKeys.typename.toString, "").asInstanceOf[String]
        val containerVersionCsv : String = containerInstanceMap.getOrElse(CsvContainerInterfaceKeys.version.toString, "").asInstanceOf[String]
        val containerPhyNameCsv : String = containerInstanceMap.getOrElse(CsvContainerInterfaceKeys.physicalname.toString, "").asInstanceOf[String]

        if (containerNameCsv.size == 0) {
            throw new MissingPropertyException("the supplied byte array to deserialize does not have a known container name.", null)
        }

        /** get an empty ContainerInterface instance for this type name from the objResolver */
        val ci : ContainerInterface = objResolver.getInstance(classLoader, containerNameCsv)
        if (ci == null) {
            throw new ObjectNotFoundException(s"type name $containerNameCsv could not be resolved and built for deserialize",null)
        }

        /** get the fields information */
        val containerBaseType : BaseTypeDef = mgr.ActiveType(containerNameCsv)
        val containerType : ContainerTypeDef = if (containerBaseType != null) containerBaseType.asInstanceOf[ContainerTypeDef] else null
        if (containerType == null) {
            throw new ObjectNotFoundException(s"type name $containerNameCsv is not a container type... deserialize fails.",null)
        }
        val fieldTypes : Array[BaseTypeDef] = containerType.ElementTypes
        fieldTypes.foreach(fieldType => {

            val fieldsCsv : Any = containerInstanceMap.getOrElse(fieldType.FullName, null)
            val isContainerType : Boolean = isContainerTypeDef(fieldType)
            val fld : Any = if (isContainerType) {
                val containerTypeInfo : ContainerTypeDef = fieldType.asInstanceOf[ContainerTypeDef]
                createContainerType(containerTypeInfo, fieldsCsv)
            } else {
                /** currently assumed to be one of the scalars or simple types supported by json/avro */
                fieldsCsv
            }
            ci.set(fieldType.FullName, fld)
        })

        val container : ContainerInterface = null
        container
    }

    def createContainerType(containerTypeInfo : ContainerTypeDef, fieldsCsv : Any) : Any = {
        /** ContainerInterface instance? */
        val isContainerInterface : Boolean = containerTypeInfo.isInstanceOf[MappedMsgTypeDef] || containerTypeInfo.isInstanceOf[StructTypeDef]
        val containerInst : Any = if (isContainerInterface) {
            /** recurse to obtain the subcontainer */
            val containerBytes : Array[Byte] = fieldsCsv.toString.toCharArray.map(_.toByte)
            val container : ContainerInterface = deserialize(containerBytes)
            container
        } else { /** Check for collection that is currently supported */

        val coll : Any = containerTypeInfo match {
                case a : ArrayTypeDef =>  {
                    val collElements : List[Map[String, Any]] = fieldsCsv.asInstanceOf[List[Map[String, Any]]]
                    csvAsArray(containerTypeInfo, collElements)
                }
                case ab : ArrayBufTypeDef => {
                    val collElements : List[Map[String, Any]] = fieldsCsv.asInstanceOf[List[Map[String, Any]]]
                    csvAsArrayBuffer(containerTypeInfo, collElements)
                }
                case m : MapTypeDef => {
                    val collElements : Map[String, Any] = fieldsCsv.asInstanceOf[Map[String, Any]]
                    csvAsMap(containerTypeInfo, collElements)
                }
                case im : ImmutableMapTypeDef =>  {
                    val collElements : Map[String, Any] = fieldsCsv.asInstanceOf[Map[String, Any]]
                    csvAsMutableMap(containerTypeInfo, collElements)
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
    def csvAsArray(arrayTypeInfo : ContainerTypeDef, collElements : List[Map[String,Any]]) : Array[Any] = {

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
    def csvAsArrayBuffer(arrayTypeInfo : ContainerTypeDef, collElements : List[Map[String,Any]]) : ArrayBuffer[Any] = {

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
    def csvAsMap(mapTypeInfo : ContainerTypeDef, collElements : Map[String,Any]) : scala.collection.immutable.Map[Any,Any] = {
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
    def csvAsMutableMap(mapTypeInfo : ContainerTypeDef, collElements : Map[String,Any]) : scala.collection.mutable.Map[Any,Any] = {
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
      * @param configCsv
      * @return Map[String, Any]
      */

    @throws(classOf[com.ligadata.Exceptions.EngineConfigParsingException])
    def csvStringAsMap(configCsv: String): Map[String, Any] = {
        try {
            //implicit val jsonFormats: Formats = DefaultFormats
            //val json = parse(configCsv)
            logger.debug("Parsed the json : " + configCsv)

            //val fullmap = json.values.asInstanceOf[Map[String, Any]]

            //fullmap
            null
        } catch {
            case e: Exception => {
                logger.debug("", e)
                throw EngineConfigParsingException(e.getMessage, e)
            }
        }
    }



}

