package com.ligadata.KamanjaBase;
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.Formats
import scala.xml.XML
import scala.xml.Elem
import com.ligadata.KamanjaBase.{InputData, DelimitedData, JsonData, XmlData, KvData}
import com.ligadata.BaseTypes._
import com.ligadata.KamanjaBase.SerializeDeserialize
import java.io.{ DataInputStream, DataOutputStream , ByteArrayOutputStream}
import org.apache.logging.log4j.{ Logger, LogManager }
import java.util.Date
import com.ligadata.KamanjaBase.{BaseMsg, BaseMsgObj, TransformMessage, BaseContainer, MdBaseResolveInfo, MessageContainerBase, RDDObject, RDD, JavaRDDObject}

object KamanjaExceptionEvent extends RDDObject[KamanjaExceptionEvent] with BaseMsgObj {
  override def TransformDataAttributes: TransformMessage = null
  override def NeedToTransformData: Boolean = false
  override def FullName: String = "com.ligadata.KamanjaBase.KamanjaExceptionEvent"
  override def NameSpace: String = "com.ligadata.KamanjaBase"
  override def Name: String = "KamanjaExceptionEvent"
  override def Version: String = "000001.000002.000000"
  override def CreateNewMessage: BaseMsg  = new KamanjaExceptionEvent()
  override def IsFixed:Boolean = true;
  override def IsKv:Boolean = false;
  override def CanPersist: Boolean = false;


  type T = KamanjaExceptionEvent
  override def build = new T
  override def build(from: T) = new T(from.transactionId, from)
  override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this)



  val partitionKeys : Array[String] =  Array[String]();
  override def PartitionKeyData(inputdata:InputData): Array[String] = Array[String]()
  val primaryKeys : Array[String] = Array[String]();
  override def PrimaryKeyData(inputdata:InputData): Array[String] = Array[String]()

  def getTimePartitionInfo: (String, String, String) = { // Column, Format & Types
    return(null, null, null)
  }
  override def TimePartitionData(inputdata: InputData): Long = 0



  override def hasPrimaryKey(): Boolean = {
    if(primaryKeys == null) return false;
    (primaryKeys.size > 0);
  }

  override def hasPartitionKey(): Boolean = {
    if(partitionKeys == null) return false;
    (partitionKeys.size > 0);
  }

  override def hasTimeParitionInfo(): Boolean = {
    val tmPartInfo = getTimePartitionInfo
    (tmPartInfo != null && tmPartInfo._1 != null && tmPartInfo._2 != null && tmPartInfo._3 != null);
  }



  override def getFullName = FullName

}

class KamanjaExceptionEvent(var transactionId: Long, other: KamanjaExceptionEvent) extends BaseMsg {
  override def IsFixed : Boolean = KamanjaExceptionEvent.IsFixed;
  override def IsKv : Boolean = KamanjaExceptionEvent.IsKv;

  override def CanPersist: Boolean = KamanjaExceptionEvent.CanPersist;

  override def FullName: String = KamanjaExceptionEvent.FullName
  override def NameSpace: String = KamanjaExceptionEvent.NameSpace
  override def Name: String = KamanjaExceptionEvent.Name
  override def Version: String = KamanjaExceptionEvent.Version


  var componentname:String = _ ;
  var timeoferrorepochms:Long = _ ;
  var errortype:String = _ ;
  var errorstring:String = _ ;

  override def PartitionKeyData: Array[String] = Array[String]()

  override def PrimaryKeyData: Array[String] = Array[String]()

  override def set(key: String, value: Any): Unit = { throw new Exception("set function is not yet implemented") }

  override def get(key: String): Any = {
    try {
      // Try with reflection
      return getWithReflection(key)
    } catch {
      case e: Exception => {
        LOG.debug("", e)
        // Call By Name
        return getByName(key)
      }
    }
  }
  override def getOrElse(key: String, default: Any): Any = { throw new Exception("getOrElse function is not yet implemented") }

  private def getByName(key: String): Any = {
    try {
      if(key.equals("componentname")) return componentname;
      if(key.equals("timeoferrorepochms")) return timeoferrorepochms;
      if(key.equals("errortype")) return errortype;
      if(key.equals("errorstring")) return errorstring;
      if(key.equals("transactionId")) return transactionId;

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
    val fieldX = ru.typeOf[KamanjaExceptionEvent].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
    val fmX = im.reflectField(fieldX)
    fmX.get
  }

  private val LOG = LogManager.getLogger(getClass)

  override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = { }


  override def GetMessage(childPath: Array[(String, String)], primaryKey:Array[String]): com.ligadata.KamanjaBase.BaseMsg = {
    return null
  }


  override def Save: Unit = {
    KamanjaExceptionEvent.saveOne(this)
  }

  var nativeKeyMap  =  scala.collection.mutable.Map[String, String](("componentname", "ComponentName"), ("timeoferrorepochms", "TimeOfErrorEpochMs"), ("errortype", "ErrorType"), ("errorstring", "ErrorString"))

  override def getNativeKeyValues(): scala.collection.immutable.Map[String, (String, Any)] = {

    var keyValues: scala.collection.mutable.Map[String, (String, Any)] = scala.collection.mutable.Map[String, (String, Any)]()

    try {
      keyValues("componentname") = ("ComponentName", componentname);
      keyValues("timeoferrorepochms") = ("TimeOfErrorEpochMs", timeoferrorepochms);
      keyValues("errortype") = ("ErrorType", errortype);
      keyValues("errorstring") = ("ErrorString", errorstring);

    } catch {
      case e: Exception => {
        LOG.debug("", e)
        throw e
      }
    }
    return keyValues.toMap
  }

  def populate(inputdata:InputData) = {
    if (inputdata.isInstanceOf[DelimitedData])
      populateCSV(inputdata.asInstanceOf[DelimitedData])
    else if (inputdata.isInstanceOf[JsonData])
      populateJson(inputdata.asInstanceOf[JsonData])
    else if (inputdata.isInstanceOf[XmlData])
      populateXml(inputdata.asInstanceOf[XmlData])
    else if (inputdata.isInstanceOf[KvData])
      populateKvData(inputdata.asInstanceOf[KvData])
    else throw new Exception("Invalid input data")
    timePartitionData = ComputeTimePartitionData

  }

  private def populateCSV(inputdata:DelimitedData): Unit = {
    val list = inputdata.tokens
    val arrvaldelim = inputdata.delimiters.valueDelimiter
    try{
      if(list.size < 4) throw new Exception("Incorrect input data size")
      componentname = com.ligadata.BaseTypes.StringImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos+1;
      timeoferrorepochms = com.ligadata.BaseTypes.LongImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos+1;
      errortype = com.ligadata.BaseTypes.StringImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos+1;
      errorstring = com.ligadata.BaseTypes.StringImpl.Input(list(inputdata.curPos));
      inputdata.curPos = inputdata.curPos+1;

    }catch{
      case e:Exception =>{
        LOG.debug("", e)
        throw e
      }
    }
  }

  private def populateJson(json:JsonData) : Unit = {
    try{
      if (json == null || json.cur_json == null || json.cur_json == None) throw new Exception("Invalid json data")
      assignJsonData(json)
    }catch{
      case e:Exception =>{
        LOG.debug("", e)
        throw e
      }
    }
  }

  def CollectionAsArrString(v: Any): Array[String] = {
    if (v.isInstanceOf[Set[_]]) {
      return v.asInstanceOf[Set[String]].toArray
    }
    if (v.isInstanceOf[List[_]]) {
      return v.asInstanceOf[List[String]].toArray
    }
    if (v.isInstanceOf[Array[_]]) {
      return v.asInstanceOf[Array[String]].toArray
    }
    throw new Exception("Unhandled Collection")
  }

  private def assignJsonData(json: JsonData) : Unit =  {
    type tList = List[String]
    type tMap = Map[String, Any]
    var list : List[Map[String, Any]] = null
    try{
      val mapOriginal = json.cur_json.get.asInstanceOf[Map[String, Any]]
      if (mapOriginal == null)
        throw new Exception("Invalid json data")

      val map : scala.collection.mutable.Map[String, Any] =  scala.collection.mutable.Map[String, Any]()
      mapOriginal.foreach(kv => {map(kv._1.toLowerCase()) = kv._2 } )

      componentname = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("componentname", "").toString);
      timeoferrorepochms = com.ligadata.BaseTypes.LongImpl.Input(map.getOrElse("timeoferrorepochms", 0).toString);
      errortype = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("errortype", "").toString);
      errorstring = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("errorstring", "").toString);

    }catch{
      case e:Exception =>{
        LOG.debug("", e)
        throw e
      }
    }
  }

  private def populateXml(xmlData:XmlData) : Unit = {
    try{
      val xml = XML.loadString(xmlData.dataInput)
      if(xml == null) throw new Exception("Invalid xml data")
      val _componentnameval_  = (xml \\ "componentname").text.toString
      if (_componentnameval_  != "")		componentname =  com.ligadata.BaseTypes.StringImpl.Input( _componentnameval_ ) else componentname = "";
      val _timeoferrorepochmsval_  = (xml \\ "timeoferrorepochms").text.toString
      if (_timeoferrorepochmsval_  != "")		timeoferrorepochms =  com.ligadata.BaseTypes.LongImpl.Input( _timeoferrorepochmsval_ ) else timeoferrorepochms = 0;
      val _errortypeval_  = (xml \\ "errortype").text.toString
      if (_errortypeval_  != "")		errortype =  com.ligadata.BaseTypes.StringImpl.Input( _errortypeval_ ) else errortype = "";
      val _errorstringval_  = (xml \\ "errorstring").text.toString
      if (_errorstringval_  != "")		errorstring =  com.ligadata.BaseTypes.StringImpl.Input( _errorstringval_ ) else errorstring = "";

    }catch{
      case e:Exception =>{
        LOG.debug("", e)
        throw e
      }
    }
  }

  private def populateKvData(kvData: KvData): Unit = {
    try{

      if (kvData == null)
        throw new Exception("Invalid KvData")

      val map: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String]();
      kvData.dataMap.foreach(kv => { map(kv._1.toLowerCase()) = kv._2 });

      componentname = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("componentname", "").toString);
      timeoferrorepochms = com.ligadata.BaseTypes.LongImpl.Input(map.getOrElse("timeoferrorepochms", 0).toString);
      errortype = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("errortype", "").toString);
      errorstring = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("errorstring", "").toString);

    }catch{
      case e:Exception =>{
        LOG.debug("", e)
        throw e
      }
    }
  }

  override def Serialize(dos: DataOutputStream) : Unit = {
    try {
      com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos,componentname);
      com.ligadata.BaseTypes.LongImpl.SerializeIntoDataOutputStream(dos,timeoferrorepochms);
      com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos,errortype);
      com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos,errorstring);
      com.ligadata.BaseTypes.LongImpl.SerializeIntoDataOutputStream(dos,transactionId);

      com.ligadata.BaseTypes.LongImpl.SerializeIntoDataOutputStream(dos, timePartitionData);
    } catch {
      case e: Exception => {
        LOG.debug("", e)
      }
    }
  }

  override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = {
    try {
      if (savedDataVersion == null || savedDataVersion.trim() == "")
        throw new Exception("Please provide Data Version")

      val prevVer = savedDataVersion.replaceAll("[.]", "").toLong
      val currentVer = Version.replaceAll("[.]", "").toLong


      if(prevVer == currentVer){
        componentname = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis);
        timeoferrorepochms = com.ligadata.BaseTypes.LongImpl.DeserializeFromDataInputStream(dis);
        errortype = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis);
        errorstring = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis);
        transactionId = com.ligadata.BaseTypes.LongImpl.DeserializeFromDataInputStream(dis);

        timePartitionData = com.ligadata.BaseTypes.LongImpl.DeserializeFromDataInputStream(dis)


      } else throw new Exception("Current Message/Container Version "+currentVer+" should be greater than Previous Message Version " +prevVer + "." )

    } catch {
      case e: Exception => {
        LOG.debug("", e)
      }
    }
  }

  def ConvertPrevToNewVerObj(obj : Any) : Unit = { }

  def withcomponentname(value: String) : KamanjaExceptionEvent = {
    this.componentname = value
    return this
  }

  def withtimeoferrorepochms(value: Long) : KamanjaExceptionEvent = {
    this.timeoferrorepochms = value
    return this
  }

  def witherrortype(value: String) : KamanjaExceptionEvent = {
    this.errortype = value
    return this
  }

  def witherrorstring(value: String) : KamanjaExceptionEvent = {
    this.errorstring = value
    return this
  }

  private def fromFunc(other: KamanjaExceptionEvent): KamanjaExceptionEvent = {
    componentname = com.ligadata.BaseTypes.StringImpl.Clone(other.componentname);
    timeoferrorepochms = com.ligadata.BaseTypes.LongImpl.Clone(other.timeoferrorepochms);
    errortype = com.ligadata.BaseTypes.StringImpl.Clone(other.errortype);
    errorstring = com.ligadata.BaseTypes.StringImpl.Clone(other.errorstring);


    timePartitionData = com.ligadata.BaseTypes.LongImpl.Clone(other.timePartitionData);
    return this
  }

  def ComputeTimePartitionData: Long = {
    val tmPartInfo = KamanjaExceptionEvent.getTimePartitionInfo
    if (tmPartInfo == null) return 0;
    KamanjaExceptionEvent.ComputeTimePartitionData(" ", tmPartInfo._2, tmPartInfo._3)
  }

  def getTimePartition() : KamanjaExceptionEvent = {
    timePartitionData = ComputeTimePartitionData
    return this;
  }



  if (other != null && other != this) {
    // call copying fields from other to local variables
    fromFunc(other)
  }

  def this(txnId: Long) = {
    this(txnId, null)
  }
  def this(other: KamanjaExceptionEvent) = {
    this(0, other)
  }
  def this() = {
    this(0, null)
  }
  override def Clone(): MessageContainerBase = {
    KamanjaExceptionEvent.build(this)
  }


  override def hasPrimaryKey(): Boolean = {
    KamanjaExceptionEvent.hasPrimaryKey;
  }

  override def hasPartitionKey(): Boolean = {
    KamanjaExceptionEvent.hasPartitionKey;
  }

  override def hasTimeParitionInfo(): Boolean = {
    KamanjaExceptionEvent.hasTimeParitionInfo;
  }

}
