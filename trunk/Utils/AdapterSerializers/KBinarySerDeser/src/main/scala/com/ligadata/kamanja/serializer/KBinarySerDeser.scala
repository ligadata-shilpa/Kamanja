package org.kamanja.serdeser.kbinary

import com.ligadata.BaseTypes._

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.collection.JavaConverters._
import java.io._
import java.util.zip.{CRC32, Checksum}

import org.apache.logging.log4j._
import com.ligadata.Exceptions._
import com.ligadata.KamanjaBase._
import com.ligadata.BaseTypes
import com.ligadata.KamanjaBase.AttributeTypeInfo.TypeCategory
import com.ligadata.KamanjaBase.AttributeTypeInfo.TypeCategory._

/**
  * KBinarySerDeser instance can serialize a ContainerInterface to a byte array and deserialize a byte array to form
  * an instance of the ContainerInterface encoded in its bytes.
  *
  * Pre-condition: The JSONSerDes must be initialized with the metadata manager, object resolver and class loader
  * before it can be used.
  */
class KBinarySerDeser extends SerializeDeserialize {

    var _objResolver : ObjectResolver = null
    var _config : Map[String,String] = Map[String,String]()
    var _isReady : Boolean = false


    case class ContainerHeader(schemaId: Int, fieldCnt: Int, isFixed: Boolean, dataSize: Int, checksumVal: Int);
    case class ArrayHeader(itmType: TypeCategory, cnt: Int)
    case class MapHeader(keyType: TypeCategory, valType: TypeCategory, cnt: Int)

    // write container header:
    // header: schemaId: Int, fieldCnt: Int, isFixed: Bool, serializedSize:Int, checksum: Int
    private def WriteContainerHeader(dos: DataOutputStream, h : ContainerHeader) = {
        dos.writeLong(h.schemaId)
        dos.writeInt(h.fieldCnt)
        dos.writeBoolean(h.isFixed)
        dos.writeInt(h.dataSize)
        dos.writeInt(h.checksumVal)
    }
    // Read container header:
    private def ReadContainerHeader(dis: DataInputStream) : ContainerHeader= {
        val schemaId = dis.readInt
        val fieldCnt = dis.readInt
        val isFixed = dis.readBoolean
        val dataSize = dis.readInt
        val checksumValue = dis.readInt
        ContainerHeader(schemaId, fieldCnt, isFixed, dataSize, checksumValue)
    }

    private def ReadArrayHeader(dis: DataInputStream) : ArrayHeader = {
        val vt = AttributeTypeInfo.toTypeCategory(dis.readShort)
        val cnt = dis.readInt
        ArrayHeader(vt, cnt)
    }

    private def WriteArrayHeader(dos: DataOutputStream, ah: ArrayHeader) = {
        dos.writeShort(ah.itmType.getValue.toShort)
        dos.writeInt(ah.cnt)
    }

    private def WriteMapHeader(dos: DataOutputStream, mh: MapHeader) = {
        dos.writeShort(mh.keyType.getValue.toShort)
        dos.writeShort(mh.valType.getValue.toShort)
        dos.writeInt(mh.cnt)
    }

    private def ReadMapHeader(dis: DataInputStream) : MapHeader = {
        val kt = AttributeTypeInfo.toTypeCategory(dis.readShort)
        val vt = AttributeTypeInfo.toTypeCategory(dis.readShort)
        val cnt = dis.readInt
        MapHeader(kt, vt, cnt)
    }
    private def WriteFldInfo(dos : DataOutputStream, tc : TypeCategory, name: String) = {
        dos.writeShort(tc.getValue.toShort)
        dos.writeUTF(name)
    }

    private def WriteFldInfo(dos : DataOutputStream, fld: AttributeValue) =
        WriteFldInfo(dos, fld.getValueType.getTypeCategory, fld.getValueType.getName)

    private def ReadFldInfo(dis : DataInputStream) : (TypeCategory, String) = {
        val tc = dis.readShort()
        val name = dis.readUTF()
        val typeCategory = AttributeTypeInfo.toTypeCategory(tc)
        (typeCategory, name)
    }

    /**
      * Serialize the supplied container to a byte array using Binary format
      *
      * @param v a ContainerInterface (describes a standard kamanja container)
      */
    @throws(classOf[com.ligadata.Exceptions.ObjectNotFoundException])
    @throws(classOf[com.ligadata.Exceptions.UnsupportedObjectException])
    def serialize(v : ContainerInterface) : Array[Byte] = {
        val bos: ByteArrayOutputStream = new ByteArrayOutputStream(8 * 1024)
        val dos = new DataOutputStream(bos)

        serializeContainer(dos, v)

        dos.close()
        bos.toByteArray
    }

    private def serializeContainer(dosFinal: DataOutputStream, v : ContainerInterface) : Unit = {
        val bos: ByteArrayOutputStream = new ByteArrayOutputStream(8 * 1024)
        val dos = new DataOutputStream(bos)

        val fields = v.getAllAttributeValues
        val fieldCnt = fields.length
        val schemaId = v.getSchemaId
        val isFixed = v.isFixed
        var processCnt : Int = 0
        fields.foreach(fld => {
            processCnt += 1
            val valueType = fld.getValueType
            val rawValue: Any = fld.getValue
            if(! isFixed)
                WriteFldInfo(dos, fld)
            valueType.getTypeCategory match {
                case MAP => mapAsKBinary(dos, valueType, rawValue.asInstanceOf[Map[Any, Any]])
                case ARRAY => arrayAsKBinary(dos, valueType, rawValue.asInstanceOf[Array[Any]])
                case (MESSAGE | CONTAINER) => serializeContainer(dos, rawValue.asInstanceOf[ContainerInterface])
                case (BOOLEAN | BYTE | LONG | DOUBLE | FLOAT | INT | STRING) => WriteVal(dos, valueType.getTypeCategory, rawValue)
                case _ => throw new UnsupportedObjectException(s"container type ${valueType.getName} not currently serializable", null)
            }
        })
        val dataSize = dos.size
        dos.close()
        val bytes = bos.toByteArray

        // prepare header..
        // header: schemaId: Int, fieldCnt: Int, isFixed: Bool, serializedSize:Int, checksum: Int
        val checksum = new CRC32
        checksum.update(bytes, 0, bytes.length);
        val checksumValue = checksum.getValue().toInt;

        WriteContainerHeader(dosFinal, ContainerHeader(schemaId, fieldCnt, isFixed, dataSize, checksumValue))
        // data: byte array
        dosFinal.write(bytes)
    }

    private def WriteVal(dos: DataOutputStream, typeCategory: TypeCategory, v: Any) =
        typeCategory match {
            case BOOLEAN => dos.writeBoolean(v.asInstanceOf[Boolean])
            case BYTE => dos.writeByte(v.asInstanceOf[Byte])
            case LONG => dos.writeLong(v.asInstanceOf[Long])
            case INT => dos.writeInt(v.asInstanceOf[Int])
            case FLOAT => dos.writeFloat(v.asInstanceOf[Float])
            case DOUBLE => dos.writeDouble(v.asInstanceOf[Double])
            case STRING => dos.writeUTF(v.asInstanceOf[String])
            case _ => throw new UnsupportedObjectException(s"KBinary WriteVal got unsupported type, typeId: ${typeCategory.name}", null)
        }

    private def ReadVal(dis: DataInputStream, typeCategory: TypeCategory) =
        typeCategory match {
            case BOOLEAN => dis.readBoolean
            case BYTE => dis.readByte
            case LONG => dis.readLong
            case INT => dis.readInt
            case FLOAT => dis.readFloat
            case DOUBLE => dis.readDouble
            case STRING => dis.readUTF
            case _ => throw new UnsupportedObjectException(s"KBinary ReadVal got unsupported type, typeId: ${typeCategory.name}", null)
        }

    /**
      * Create a kbinary from the supplied immutable map.  Note that either/both map elements can in turn be
      * containers.  As a pre-condition, the map cannot be null and can have zero to N key/value pairs.
      *
      * @param dos the output stream to receive the value
      * @param map the map instance
      * @return Unit
      */
    @throws(classOf[IOException])
    private def mapAsKBinary(dos : DataOutputStream, attribType : AttributeTypeInfo, map : scala.collection.mutable.Map[Any,Any]) : Unit = {
        mapAsKBinary(dos, attribType.getKeyTypeCategory, attribType.getValTypeCategory, map)
    }

    @throws(classOf[IOException])
    private def mapGenericAsKBinary(dos : DataOutputStream, map : scala.collection.mutable.Map[Any,Any]) : Unit = {
        val (keyType, valType) = mapElemTypes(map)
        mapAsKBinary(dos, keyType, valType, map)
    }

    private def mapAsKBinary(dos : DataOutputStream, keyType: TypeCategory, valType: TypeCategory, map : scala.collection.mutable.Map[Any,Any]) : Unit = {
        if(map != null) {
            keyType match {
                case (BYTE | LONG | INT | STRING) => ;
                case _ => throw new UnsupportedObjectException(s"KBinary serialize doesn't support maps as with complex key types, keyType: ${keyType.name}", null)
            }
            if(valType == NONE)
                throw new UnsupportedObjectException(s"KBinary serialize doesn't support nested maps other than simple value types", null)
        }

        WriteMapHeader(dos, MapHeader(keyType, valType, if(map == null) 0 else map.size))
        if(map == null)
            return
        var idx = 0
        map.foreach(pair => {
            val k = pair._1
            val v = pair._2
            idx += 1
            WriteVal(dos, keyType, k)
            valType match {
                case (BOOLEAN | BYTE | LONG | INT | FLOAT | DOUBLE | STRING) => WriteVal(dos, valType, v)
                case MAP => mapGenericAsKBinary(dos, v.asInstanceOf[scala.collection.mutable.Map[Any,Any]])
                case ARRAY => arrayGenericAsKBinary(dos, v.asInstanceOf[Array[Any]])
                case (CONTAINER | MESSAGE) => serializeContainer(dos, v.asInstanceOf[ContainerInterface])
            }
        })
    }

    /**
      * Create a Json string from the supplied array.  Note that the array elements can themselves
      * be containers.  As a pre-condition, the array cannot be null and can have zero to N items.
      *
      * @param dos the output stream to receive the value
      * @param array the array instance
      * @return Unit
      */
    @throws(classOf[IOException])
    private def arrayAsKBinary(dos : DataOutputStream, attribType : AttributeTypeInfo, array : Array[Any]) : Unit = {
        val itmType = attribType.getTypeCategory
        val memberCnt = if(array == null) 0 else array.size
        WriteArrayHeader(dos, ArrayHeader(itmType, memberCnt))

        if(array != null) {
            array.foreach(itm => {
                itmType match {
                    case (BOOLEAN | BYTE | LONG | INT | FLOAT | DOUBLE | STRING) => WriteVal(dos, itmType, itm)
                    case MAP => mapGenericAsKBinary(dos, itm.asInstanceOf[scala.collection.mutable.Map[Any, Any]])
                    case ARRAY => arrayGenericAsKBinary(dos, itm.asInstanceOf[Array[Any]])
                    case (CONTAINER | MESSAGE) => serializeContainer(dos, itm.asInstanceOf[ContainerInterface])
                }
            })
        }
    }

    private def mapElemTypes(map: scala.collection.mutable.Map[Any,Any]) : (TypeCategory, TypeCategory) = {
        if(map == null) (NONE, NONE)
        else if(map.isInstanceOf[scala.collection.mutable.Map[String,String]]) (STRING, STRING)
        else if(map.isInstanceOf[scala.collection.mutable.Map[Int,String]]) (INT, STRING)
        else if(map.isInstanceOf[scala.collection.mutable.Map[Long,String]]) (LONG, STRING)
        else if(map.isInstanceOf[scala.collection.mutable.Map[Long,Long]]) (LONG, LONG)
        else if(map.isInstanceOf[scala.collection.mutable.Map[Int,Int]]) (INT, INT)
        else if(map.isInstanceOf[scala.collection.mutable.Map[Int,Long]]) (INT, LONG)
        else (NONE, NONE)
    }

    private def arrayElemType(array: Array[Any]) : TypeCategory = {
        if(array == null) NONE
        else if(array.isInstanceOf[Int])     INT
        else if(array.isInstanceOf[Boolean]) BOOLEAN
        else if(array.isInstanceOf[Byte])    BYTE
        else if(array.isInstanceOf[Long])    LONG
        else if(array.isInstanceOf[Double])  DOUBLE
        else if(array.isInstanceOf[Float])   FLOAT
        else if(array.isInstanceOf[String])  STRING
        else NONE
    }

    private def arrayGenericAsKBinary(dos : DataOutputStream, array : Array[Any]) : Unit = {
        val memberCnt = if(array == null) 0 else array.size
        val itmType = arrayElemType(array)
        if((array != null) && (itmType == NONE)) {
            case _ => throw new UnsupportedObjectException(s"container type ${itmType.name()} not currently serializable", null)
        }
        WriteArrayHeader(dos, ArrayHeader(itmType, memberCnt))
        if(array != null)
            array.foreach(itm => WriteVal(dos, itmType, itm))
    }

    /**
      * Set the object resolver to be used for this serializer
      *
      * @param objRes an ObjectResolver
      */
    def setObjectResolver(objRes : ObjectResolver) : Unit = {
        _objResolver = objRes
    }

    /**
      * Configure the SerializeDeserialize adapter.  This must be done before the adapter implementation can be used.
      *
      * @param objResolver the ObjectResolver instance that can instantiate ContainerInterface instances
      * @param config a map of options that might be used to configure the execution of this SerializeDeserialize instance. This may
      *               be null if it is not necessary for the SerializeDeserialize implementation
      */
    def configure(objResolver: ObjectResolver, config : java.util.Map[String,String]): Unit = {
        _objResolver = objResolver
        _config = if (config != null) config.asScala else Map[String,String]()
        _isReady = _objResolver != null && _config != null
    }

    /**
      * Deserialize the supplied byte array into some ContainerInterface instance.
      *
      * @param b the byte array containing the serialized ContainerInterface instance
      * @return a ContainerInterface
      */
    def deserialize(b: Array[Byte], containerName: String) : ContainerInterface = {
        var dis = new DataInputStream(new ByteArrayInputStream(b));
        val container = ReadContainer(dis)
        container
    }

    /**
      * Deserialize the supplied byte array into some ContainerInterface instance.
      *
      * @param dis the DataInputStream that contains the container
      * @return a ContainerInterface
      */
    @throws(classOf[com.ligadata.Exceptions.ObjectNotFoundException])
    @throws(classOf[com.ligadata.Exceptions.MissingPropertyException])
    @throws(classOf[com.ligadata.Exceptions.UnsupportedObjectException])
    @throws(classOf[IOException])
    private def ReadContainer(dis : DataInputStream) : ContainerInterface = {
        val hdr = ReadContainerHeader(dis)
        val ci = _objResolver.getInstance(hdr.schemaId)
        if (ci == null) {
            throw new ObjectNotFoundException(s"schemaid ${hdr.schemaId} could not be resolved for deserialize",null)
        }
        val attribs = ci.getAttributeTypes
        for (idx <- 0 to hdr.fieldCnt) {
            var attribType: TypeCategory = NONE
            var attribName = ""

            if (hdr.isFixed)
                attribType = attribs(idx).getTypeCategory
            else {
                (attribType, attribName) = ReadFldInfo(dis)
            }
            attribType match {
                case MAP => ReadMap(dis)
                case ARRAY => ReadArray(dis)
                case (MESSAGE | CONTAINER) => ReadContainer(dis)
                case (BOOLEAN | BYTE | LONG | DOUBLE | FLOAT | INT | STRING) => ReadVal(dis, attribType)
                case _ => throw new UnsupportedObjectException(s"container type ${attribType.name} not currently serializable", null)
            }
        }
        ci
    }

    private def ReadMap(dis : DataInputStream) : scala.collection.mutable.Map[Any,Any] = {
        val hdr = ReadMapHeader(dis)
        null
    }

    private def ReadArray(dis : DataInputStream) : Array[Any] = {
        val hdr = ReadArrayHeader(dis)
        null
    }
}

