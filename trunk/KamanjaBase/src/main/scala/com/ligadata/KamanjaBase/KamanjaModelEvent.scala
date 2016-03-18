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

object KamanjaModelEvent extends RDDObject[KamanjaModelEvent] with BaseMsgObj {
	override def TransformDataAttributes: TransformMessage = null
	override def NeedToTransformData: Boolean = false
	override def FullName: String = "com.ligadata.KamanjaBase.KamanjaModelEvent"
	override def NameSpace: String = "com.ligadata.KamanjaBase"
	override def Name: String = "KamanjaModelEvent"
	override def Version: String = "000001.000002.000000"
	override def CreateNewMessage: BaseMsg  = new KamanjaModelEvent()
	override def IsFixed:Boolean = true;
	override def IsKv:Boolean = false;
	override def CanPersist: Boolean = false;


	type T = KamanjaModelEvent
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

class KamanjaModelEvent(var transactionId: Long, other: KamanjaModelEvent) extends BaseMsg {
	override def IsFixed : Boolean = KamanjaModelEvent.IsFixed;
	override def IsKv : Boolean = KamanjaModelEvent.IsKv;

	override def CanPersist: Boolean = KamanjaModelEvent.CanPersist;

	override def FullName: String = KamanjaModelEvent.FullName
	override def NameSpace: String = KamanjaModelEvent.NameSpace
	override def Name: String = KamanjaModelEvent.Name
	override def Version: String = KamanjaModelEvent.Version


	var modelid:Long = _ ;
	var elapsedtimeinms:Float = _ ;
	var eventepochtime:Long = _ ;
	var isresultproduced:Boolean = _ ;
	var producedmessages: scala.Array[String] = _ ;
	var error:String = _ ;

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
			if(key.equals("modelid")) return modelid;
			if(key.equals("elapsedtimeinms")) return elapsedtimeinms;
			if(key.equals("eventepochtime")) return eventepochtime;
			if(key.equals("isresultproduced")) return isresultproduced;
			if(key.equals("producedmessages")) return producedmessages;
			if(key.equals("error")) return error;
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
		val fieldX = ru.typeOf[KamanjaModelEvent].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
		val fmX = im.reflectField(fieldX)
		fmX.get
	}

	private val LOG = LogManager.getLogger(getClass)

	override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = { }


	override def GetMessage(childPath: Array[(String, String)], primaryKey:Array[String]): com.ligadata.KamanjaBase.BaseMsg = {
		return null
	}


	override def Save: Unit = {
		KamanjaModelEvent.saveOne(this)
	}

	var nativeKeyMap  =  scala.collection.mutable.Map[String, String](("modelid", "modelId"), ("elapsedtimeinms", "ElapsedTimeInMs"), ("eventepochtime", "eventEpochTime"), ("isresultproduced", "isResultProduced"), ("producedmessages", "ProducedMessages"), ("error", "Error"))

	override def getNativeKeyValues(): scala.collection.immutable.Map[String, (String, Any)] = {

		var keyValues: scala.collection.mutable.Map[String, (String, Any)] = scala.collection.mutable.Map[String, (String, Any)]()

		try {
			keyValues("modelid") = ("modelId", modelid);
			keyValues("elapsedtimeinms") = ("ElapsedTimeInMs", elapsedtimeinms);
			keyValues("eventepochtime") = ("eventEpochTime", eventepochtime);
			keyValues("isresultproduced") = ("isResultProduced", isresultproduced);
			keyValues("producedmessages") = ("ProducedMessages", producedmessages);
			keyValues("error") = ("Error", error);

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
			if(list.size < 6) throw new Exception("Incorrect input data size")
			modelid = com.ligadata.BaseTypes.LongImpl.Input(list(inputdata.curPos));
			inputdata.curPos = inputdata.curPos+1;
			elapsedtimeinms = com.ligadata.BaseTypes.FloatImpl.Input(list(inputdata.curPos));
			inputdata.curPos = inputdata.curPos+1;
			eventepochtime = com.ligadata.BaseTypes.LongImpl.Input(list(inputdata.curPos));
			inputdata.curPos = inputdata.curPos+1;
			isresultproduced = com.ligadata.BaseTypes.BoolImpl.Input(list(inputdata.curPos));
			inputdata.curPos = inputdata.curPos+1;
			producedmessages = list(inputdata.curPos).split(arrvaldelim, -1).map(v => com.ligadata.BaseTypes.StringImpl.Input(v));
			inputdata.curPos = inputdata.curPos+1
			error = com.ligadata.BaseTypes.StringImpl.Input(list(inputdata.curPos));
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

			modelid = com.ligadata.BaseTypes.LongImpl.Input(map.getOrElse("modelid", 0).toString);
			elapsedtimeinms = com.ligadata.BaseTypes.FloatImpl.Input(map.getOrElse("elapsedtimeinms", 0).toString);
			eventepochtime = com.ligadata.BaseTypes.LongImpl.Input(map.getOrElse("eventepochtime", 0).toString);
			isresultproduced = com.ligadata.BaseTypes.BoolImpl.Input(map.getOrElse("isresultproduced", false).toString);

			if (map.contains("producedmessages")){
				val arr = map.getOrElse("producedmessages", null)
				if (arr != null) {
					val arrFld = CollectionAsArrString(arr)
					producedmessages  = arrFld.map(v => com.ligadata.BaseTypes.StringImpl.Input(v.toString)).toArray
				} else producedmessages  = new scala.Array[String](0)
			}
			error = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("error", "").toString);

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
			val _modelidval_  = (xml \\ "modelid").text.toString
			if (_modelidval_  != "")		modelid =  com.ligadata.BaseTypes.LongImpl.Input( _modelidval_ ) else modelid = 0;
			val _elapsedtimeinmsval_  = (xml \\ "elapsedtimeinms").text.toString
			if (_elapsedtimeinmsval_  != "")		elapsedtimeinms =  com.ligadata.BaseTypes.FloatImpl.Input( _elapsedtimeinmsval_ ) else elapsedtimeinms = 0;
			val _eventepochtimeval_  = (xml \\ "eventepochtime").text.toString
			if (_eventepochtimeval_  != "")		eventepochtime =  com.ligadata.BaseTypes.LongImpl.Input( _eventepochtimeval_ ) else eventepochtime = 0;
			val _isresultproducedval_  = (xml \\ "isresultproduced").text.toString
			if (_isresultproducedval_  != "")		isresultproduced =  com.ligadata.BaseTypes.BoolImpl.Input( _isresultproducedval_ ) else isresultproduced = false;
			val _errorval_  = (xml \\ "error").text.toString
			if (_errorval_  != "")		error =  com.ligadata.BaseTypes.StringImpl.Input( _errorval_ ) else error = "";

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

			modelid = com.ligadata.BaseTypes.LongImpl.Input(map.getOrElse("modelid", 0).toString);
			elapsedtimeinms = com.ligadata.BaseTypes.FloatImpl.Input(map.getOrElse("elapsedtimeinms", 0).toString);
			eventepochtime = com.ligadata.BaseTypes.LongImpl.Input(map.getOrElse("eventepochtime", 0).toString);
			isresultproduced = com.ligadata.BaseTypes.BoolImpl.Input(map.getOrElse("isresultproduced", false).toString);

			if (map.contains("producedmessages")){
				val arr = map.getOrElse("producedmessages", null)
				if (arr != null) {
					producedmessages  = arr.split(kvData.delimiters.valueDelimiter, -1).map(v => com.ligadata.BaseTypes.StringImpl.Input(v.toString)).toArray
				} else producedmessages  = new scala.Array[String](0)
			}
			error = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("error", "").toString);

		}catch{
			case e:Exception =>{
				LOG.debug("", e)
				throw e
			}
		}
	}

	override def Serialize(dos: DataOutputStream) : Unit = {
		try {
			com.ligadata.BaseTypes.LongImpl.SerializeIntoDataOutputStream(dos,modelid);
			com.ligadata.BaseTypes.FloatImpl.SerializeIntoDataOutputStream(dos,elapsedtimeinms);
			com.ligadata.BaseTypes.LongImpl.SerializeIntoDataOutputStream(dos,eventepochtime);
			com.ligadata.BaseTypes.BoolImpl.SerializeIntoDataOutputStream(dos,isresultproduced);
			if (producedmessages==null || producedmessages.size == 0) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,0);
			else {
				com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,producedmessages.size);
				producedmessages.foreach(v => {
					com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos, v);
				})}
			com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos,error);
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
				modelid = com.ligadata.BaseTypes.LongImpl.DeserializeFromDataInputStream(dis);
				elapsedtimeinms = com.ligadata.BaseTypes.FloatImpl.DeserializeFromDataInputStream(dis);
				eventepochtime = com.ligadata.BaseTypes.LongImpl.DeserializeFromDataInputStream(dis);
				isresultproduced = com.ligadata.BaseTypes.BoolImpl.DeserializeFromDataInputStream(dis);
				{
					var arraySize = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
					val i:Int = 0;
					if(arraySize > 0){ producedmessages = new scala.Array[String](arraySize)
						for (i <- 0 until arraySize) {
							val inst = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis);
							producedmessages(i) = inst;
						}

					}}
				error = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis);
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

	def withmodelid(value: Long) : KamanjaModelEvent = {
		this.modelid = value
		return this
	}

	def withelapsedtimeinms(value: Float) : KamanjaModelEvent = {
		this.elapsedtimeinms = value
		return this
	}

	def witheventepochtime(value: Long) : KamanjaModelEvent = {
		this.eventepochtime = value
		return this
	}

	def withisresultproduced(value: Boolean) : KamanjaModelEvent = {
		this.isresultproduced = value
		return this
	}

	def withproducedmessages(value: scala.Array[String]) : KamanjaModelEvent = {
		this.producedmessages = value
		return this
	}

	def witherror(value: String) : KamanjaModelEvent = {
		this.error = value
		return this
	}

	private def fromFunc(other: KamanjaModelEvent): KamanjaModelEvent = {
		modelid = com.ligadata.BaseTypes.LongImpl.Clone(other.modelid);
		elapsedtimeinms = com.ligadata.BaseTypes.FloatImpl.Clone(other.elapsedtimeinms);
		eventepochtime = com.ligadata.BaseTypes.LongImpl.Clone(other.eventepochtime);
		isresultproduced = com.ligadata.BaseTypes.BoolImpl.Clone(other.isresultproduced);
		error = com.ligadata.BaseTypes.StringImpl.Clone(other.error);

		if (other.producedmessages != null ) {
			producedmessages = new scala.Array[String](other.producedmessages.length);
			producedmessages = other.producedmessages.map(v => com.ligadata.BaseTypes.StringImpl.Clone(v));
		}
		else producedmessages = null;

		timePartitionData = com.ligadata.BaseTypes.LongImpl.Clone(other.timePartitionData);
		return this
	}

	def ComputeTimePartitionData: Long = {
		val tmPartInfo = KamanjaModelEvent.getTimePartitionInfo
		if (tmPartInfo == null) return 0;
		KamanjaModelEvent.ComputeTimePartitionData(" ", tmPartInfo._2, tmPartInfo._3)
	}

	def getTimePartition() : KamanjaModelEvent = {
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
	def this(other: KamanjaModelEvent) = {
		this(0, other)
	}
	def this() = {
		this(0, null)
	}
	override def Clone(): MessageContainerBase = {
		KamanjaModelEvent.build(this)
	}


	override def hasPrimaryKey(): Boolean = {
		KamanjaModelEvent.hasPrimaryKey;
	}

	override def hasPartitionKey(): Boolean = {
		KamanjaModelEvent.hasPartitionKey;
	}

	override def hasTimeParitionInfo(): Boolean = {
		KamanjaModelEvent.hasTimeParitionInfo;
	}

}