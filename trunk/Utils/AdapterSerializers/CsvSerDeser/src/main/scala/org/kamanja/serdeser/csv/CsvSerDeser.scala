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
      *
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
      *
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
      *
      * @param valueStr string presumably that has embedded double quotes.
      * @return adjusted string
      */
    private def escapeQuotes(valueStr : String) : String = {
        val len : Int = if (valueStr != null) valueStr.length else 0
        val buffer : StringBuilder = new StringBuilder
        var base : Int = 0
        var startPoint : Int = 0
        if (valueStr != null) {
            while (startPoint >= 0) {
                startPoint = valueStr.indexOf('"', startPoint)
                if (startPoint >= 0) {
                    val aSlice: String = valueStr.slice(base, startPoint)
                    buffer.append(aSlice)
                    buffer.append(s"${'"'}${'"'}")
                    startPoint += 1 // start after the processed quote
                    base = startPoint
                    if (startPoint >= len) {
                        startPoint = -1 // end it ... avoid out of bounds
                    }
                }
            }
        }
        val escapeQuotedStr : String = if (buffer.isEmpty) valueStr else buffer.toString
        escapeQuotedStr
    }

    /**
      *
      * serDeserConfig
      * class SerializeDeserializeConfig ( var serDeserType : SerializeDeserializeType.SerDeserType
      * , var jar : String
      * , var lineDelimiter : String = "\r\n"
      * , var fieldDelimiter : String = ","
      * , var produceHeader : Boolean = false
      * , var alwaysQuoteField : Boolean = false) extends BaseElemDef {}
      */


    /**
      * Discern if the supplied BaseTypeDef is a ContainerTypeDef.  ContainerTypeDefs are used to describe
      * messages, containers, and the collection types.
      * @param aType a metadata base type
      * @return true if container
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
    @throws(classOf[com.ligadata.Exceptions.MissingPropertyException])
    @throws(classOf[com.ligadata.Exceptions.ObjectNotFoundException])
    @throws(classOf[com.ligadata.Exceptions.UnsupportedObjectException])
    def deserialize(b: Array[Byte]) : ContainerInterface = {

        val rawCsvContainerStr : String = new String(b)
        val (containerfFieldMap, containerType, containerFldTypes)
                : (Map[String, Any], ContainerTypeDef, Array[BaseTypeDef]) = dataMapAndTypesForCsvString(rawCsvContainerStr)

        /** Decode the map to produce an instance of ContainerInterface */

        /** get the container key information.. the top level object must be a ContainerInterface... if these
          * are not present, nothing good will come of it */
        val containerNameCsv : String = if (containerType != null) containerType.FullName else ""
        //val containerVersionCsv : String = containerfFieldMap.getOrElse(CsvContainerInterfaceKeys.version.toString, "").asInstanceOf[String]
        //val containerPhyNameCsv : String = containerfFieldMap.getOrElse(CsvContainerInterfaceKeys.physicalname.toString, "").asInstanceOf[String]

        if (containerNameCsv.isEmpty) {
            throw new MissingPropertyException("the supplied byte array to deserialize does not have a known container name.", null)
        }

        /** get an empty ContainerInterface instance for this type name from the objResolver */
        val ci : ContainerInterface = objResolver.getInstance(classLoader, containerNameCsv)
        if (ci == null) {
            throw new ObjectNotFoundException(s"type name $containerNameCsv could not be resolved and built for deserialize",null)
        }

        /** get the fields information */
        if (containerType == null) {
            throw new ObjectNotFoundException(s"type name $containerNameCsv is not a container type... deserialize fails.",null)
        }
        containerFldTypes.foreach(fieldType => {

            val fieldsCsv : Any = containerfFieldMap.getOrElse(fieldType.FullName, null)
            val isContainerType : Boolean = isContainerTypeDef(fieldType)
            val fld : Any = if (isContainerType) {
                val containerTypeInfo : ContainerTypeDef = fieldType.asInstanceOf[ContainerTypeDef]
                logger.error(s"field type name ${containerTypeInfo.FullName} is a container type... containers are not supported by the CSV deserializerat this time... deserializatin fails.")
                throw new UnsupportedObjectException(s"field type name ${containerTypeInfo.FullName} is a container type... containers are not supported by the CSV deserializerat this time... deserializatin fails.",null)
            } else {
                /** currently assumed to be one of the scalars or simple types supported by json/avro */
                fieldsCsv
            }
            ci.set(fieldType.FullName, fld)
        })

        val container : ContainerInterface = null
        container
    }

    /**
      * Translate the supplied CSV string to a Map[String, Any]. The expectation is that the first field is expected
      * to be the ContainerInterface's ContainerTypeDef's namespace.name.  With this name, the type is obtained from
      * the metadata so that the BaseTypeDef instances that describe each field in the supplied csv record can be
      * determined.
      *
      * @param configCsv
      * @return Map[String, Any]
      */

    @throws(classOf[com.ligadata.Exceptions.ObjectNotFoundException])
    @throws(classOf[com.ligadata.Exceptions.TypeParsingException])
    def dataMapAndTypesForCsvString(configCsv: String): (Map[String, Any], ContainerTypeDef, Array[BaseTypeDef]) = {
        val rawCsvFields : Array[String] = if (configCsv != null) {
            configCsv.split(serDeserConfig.fieldDelimiter)
        } else {
            Array[String]()
        }
        if (rawCsvFields.isEmpty) {
            logger.error("The supplied CSV record is empty...abandoning processing")
            throw new ObjectNotFoundException("The supplied CSV record is empty...abandoning processing", null)
        }

        val containerTypeName : String = rawCsvFields.head
        val containerCsvFields : Array[String] = rawCsvFields.tail

        val basetypedef : BaseTypeDef = mdMgr.ActiveType(containerTypeName)
        if (basetypedef == null) {
            logger.error("The supplied CSV record's first field that describes the container type was not found in the metadata...abandoning processing")
            throw new ObjectNotFoundException("The supplied CSV record's first field that describes the container type was not found in the metadata...abandoning processing", null)
        }
        if (! isContainerTypeDef(basetypedef)) {
            logger.error("The supplied CSV record's first field is not a container type...abandoning processing")
            throw new TypeParsingException("The supplied CSV record's first field is not a container type...abandoning processing", null)
        }
        val containerTypeDef : ContainerTypeDef = basetypedef.asInstanceOf[ContainerTypeDef]
        val fieldTypes : Array[BaseTypeDef] = containerTypeDef.ElementTypes
        if (fieldTypes == null || (fieldTypes != null && fieldTypes.isEmpty)) {
            logger.error("The supplied CSV record's container type is either not a container or has no fields...abandoning processing")
            throw new TypeParsingException("The supplied CSV record's container type is either not a container or has no fields...abandoning processing", null)
        }

        // FIXME: this doesn't support mapped messages at the moment.  How should these be handled?  For example,
        // FIXME: when there are 100 fields defined, but only 10 are to be serialized, we should be able to do that.
        // FIXME: The type info for each field needs to be present in the csv stream.

        /** Produce the Map[typename,descapedStringValue] */
        var idx : Int = -1
        val containerCsvFieldMap : Map[String, Any] = fieldTypes.map(fld => {
            idx += 1
            val descapedString : String = containerCsvFields(idx)
            (fld.FullName,descapedString)
        }).toMap

        (containerCsvFieldMap, containerTypeDef, fieldTypes)
     }

    private def stripEnclosedEscapedQuotesAsNeeded(str : String) : String = {
        val returnStr : String = if (str != null && str.size > 0 && str.contains(s"${'"'}")) {
            /** first deal with enclosed quotes */
            val strEnclQuotesGone : String = if (str.startsWith(s"${'"'}") && str.endsWith(s"${'"'}")) str.tail.dropRight(1) else str
            /** next deal with escaped quotes */
            val strDescaped : String = if (strEnclQuotesGone.contains(s"${'"'}${'"'}")) {
                descapeQuotes(strEnclQuotesGone)
            } else {
                strEnclQuotesGone
            }
            strDescaped
        } else {
            str
        }
        returnStr
    }



    /**
      * The compliment to the escapeQuotes function, *remove* enclosed quotes that may have been added and any escaped
      * quotes that may be internal to the supplied valueStr argument.
      *
      * Precondition: the supplied string has at least one instance of consecutive double quotes
      *
      * @param valueStr string presumably that may be enclosed in double quotes and could possibly have
      *                 embedded double quotes.
      * @return adjusted string
      */
    @throws(classOf[com.ligadata.Exceptions.InvalidArgumentException])
    private def descapeQuotes(valueStr : String) : String = {
        val len : Int = if (valueStr != null) valueStr.length else 0
        val buffer : StringBuilder = new StringBuilder
        var base : Int = 0
        var startPoint : Int = 0
        if (valueStr != null) {
            while (startPoint >= 0) {
                startPoint = valueStr.indexOf('"', startPoint)
                if (startPoint >= 0) {
                    val aSlice: String = valueStr.slice(base, startPoint)
                    buffer.append(aSlice)
                    buffer.append('"')
                    if (valueStr(startPoint+1) == '"')
                        startPoint += 2 // start after both quotes
                    else {
                        logger.error("The string is supposed to have consecutive double quoates... it does not... abandoning processing")
                        throw new InvalidArgumentException("The string is supposed to have consecutive double quoates... it does not... abandoning processing", null)
                    }
                    base = startPoint
                    if (startPoint >= len) {
                        startPoint = -1 // end it ... avoid out of bounds
                    }
                }
            }
        }
        val escapeQuotedStr : String = if (buffer.isEmpty) valueStr else buffer.toString
        escapeQuotedStr
    }


}

