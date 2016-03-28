package com.ligadata.KamanjaBase;
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.Formats
import scala.xml.XML
//import scala.xml.ElemKamanjaStatusEvent.scala
import com.ligadata.KamanjaBase.{InputData, DelimitedData, JsonData, XmlData, KvData}
import com.ligadata.BaseTypes._
import com.ligadata.KamanjaBase.SerializeDeserialize
import java.io.{ DataInputStream, DataOutputStream , ByteArrayOutputStream}
import org.apache.logging.log4j.{ Logger, LogManager }
import java.util.Date
import com.ligadata.KamanjaBase.{BaseMsg, BaseMsgObj, TransformMessage, BaseContainer, MdBaseResolveInfo, MessageContainerBase, RDDObject, RDD, JavaRDDObject}

object KamanjaStatusEvent extends RDDObject[KamanjaStatusEvent] with BaseMsgObj {
	override def TransformDataAttributes: TransformMessage = null
	override def NeedToTransformData: Boolean = false
	override def FullName: String = "com.ligadata.KamanjaBase.KamanjaStatusEvent"
	override def NameSpace: String = "com.ligadata.KamanjaBase"
	override def Name: String = "KamanjaStatusEvent"
	override def Version: String = "000001.000002.000000"
	override def CreateNewMessage: BaseMsg  = new KamanjaStatusEvent()
	override def IsFixed:Boolean = true;
	override def IsKv:Boolean = false;
	 override def CanPersist: Boolean = false;

 
  type T = KamanjaStatusEvent     
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

class KamanjaStatusEvent(var transactionId: Long, other: KamanjaStatusEvent) extends BaseMsg {
	override def IsFixed : Boolean = KamanjaStatusEvent.IsFixed;
	override def IsKv : Boolean = KamanjaStatusEvent.IsKv;

	 override def CanPersist: Boolean = KamanjaStatusEvent.CanPersist;

	override def FullName: String = KamanjaStatusEvent.FullName
	override def NameSpace: String = KamanjaStatusEvent.NameSpace
	override def Name: String = KamanjaStatusEvent.Name
	override def Version: String = KamanjaStatusEvent.Version


	var nodeid:String = _ ;
	var eventtime:Long = _ ;
	var statusstring:String = _ ;

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
	    	 if(key.equals("nodeid")) return nodeid; 
	 if(key.equals("eventtime")) return eventtime; 
	 if(key.equals("statusstring")) return statusstring; 
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
		  val fieldX = ru.typeOf[KamanjaStatusEvent].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
	  val fmX = im.reflectField(fieldX)
	  fmX.get
	}
    
    private val LOG = LogManager.getLogger(getClass)
    
    override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = { }
     
     
    override def GetMessage(childPath: Array[(String, String)], primaryKey:Array[String]): com.ligadata.KamanjaBase.BaseMsg = {
       return null
    } 
     
     
    override def Save: Unit = {
		 KamanjaStatusEvent.saveOne(this)
	}
 	 
    var nativeKeyMap  =  scala.collection.mutable.Map[String, String](("nodeid", "NodeId"), ("eventtime", "eventTime"), ("statusstring", "StatusString"))
    
    override def getNativeKeyValues(): scala.collection.immutable.Map[String, (String, Any)] = {

    var keyValues: scala.collection.mutable.Map[String, (String, Any)] = scala.collection.mutable.Map[String, (String, Any)]()

    try {
         	 keyValues("nodeid") = ("NodeId", nodeid); 
	 keyValues("eventtime") = ("eventTime", eventtime); 
	 keyValues("statusstring") = ("StatusString", statusstring); 
  
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
			if(list.size < 3) throw new Exception("Incorrect input data size")
	  		nodeid = com.ligadata.BaseTypes.StringImpl.Input(list(inputdata.curPos));
		inputdata.curPos = inputdata.curPos+1;
		eventtime = com.ligadata.BaseTypes.LongImpl.Input(list(inputdata.curPos));
		inputdata.curPos = inputdata.curPos+1;
		statusstring = com.ligadata.BaseTypes.StringImpl.Input(list(inputdata.curPos));
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
    
	  		 nodeid = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("nodeid", "").toString);
		 eventtime = com.ligadata.BaseTypes.LongImpl.Input(map.getOrElse("eventtime", 0).toString);
		 statusstring = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("statusstring", "").toString);

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
			val _nodeidval_  = (xml \\ "nodeid").text.toString 
			if (_nodeidval_  != "")		nodeid =  com.ligadata.BaseTypes.StringImpl.Input( _nodeidval_ ) else nodeid = "";
			val _eventtimeval_  = (xml \\ "eventtime").text.toString 
			if (_eventtimeval_  != "")		eventtime =  com.ligadata.BaseTypes.LongImpl.Input( _eventtimeval_ ) else eventtime = 0;
			val _statusstringval_  = (xml \\ "statusstring").text.toString 
			if (_statusstringval_  != "")		statusstring =  com.ligadata.BaseTypes.StringImpl.Input( _statusstringval_ ) else statusstring = "";

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
	 
	  		 nodeid = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("nodeid", "").toString);
		 eventtime = com.ligadata.BaseTypes.LongImpl.Input(map.getOrElse("eventtime", 0).toString);
		 statusstring = com.ligadata.BaseTypes.StringImpl.Input(map.getOrElse("statusstring", "").toString);

	  }catch{
  			case e:Exception =>{
          LOG.debug("", e)
   			throw e	    	
	  	}
	}
  }
	
    override def Serialize(dos: DataOutputStream) : Unit = {
        try {
    	   	com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos,nodeid);
	com.ligadata.BaseTypes.LongImpl.SerializeIntoDataOutputStream(dos,eventtime);
	com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos,statusstring);
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
              	nodeid = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis);
	eventtime = com.ligadata.BaseTypes.LongImpl.DeserializeFromDataInputStream(dis);
	statusstring = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis);
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
   
	 def withnodeid(value: String) : KamanjaStatusEvent = {
	 this.nodeid = value 
	 return this 
 	 } 

	 def witheventtime(value: Long) : KamanjaStatusEvent = {
	 this.eventtime = value 
	 return this 
 	 } 

	 def withstatusstring(value: String) : KamanjaStatusEvent = {
	 this.statusstring = value 
	 return this 
 	 } 
 
     private def fromFunc(other: KamanjaStatusEvent): KamanjaStatusEvent = {
     		nodeid = com.ligadata.BaseTypes.StringImpl.Clone(other.nodeid);
		eventtime = com.ligadata.BaseTypes.LongImpl.Clone(other.eventtime);
		statusstring = com.ligadata.BaseTypes.StringImpl.Clone(other.statusstring);

	
	timePartitionData = com.ligadata.BaseTypes.LongImpl.Clone(other.timePartitionData);
    return this
    }
    
      def ComputeTimePartitionData: Long = {
		val tmPartInfo = KamanjaStatusEvent.getTimePartitionInfo
		if (tmPartInfo == null) return 0;
		KamanjaStatusEvent.ComputeTimePartitionData(" ", tmPartInfo._2, tmPartInfo._3)
	 } 
  
    def getTimePartition() : KamanjaStatusEvent = {
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
  def this(other: KamanjaStatusEvent) = {
    this(0, other)
  }
  def this() = {
    this(0, null)
  }
  override def Clone(): MessageContainerBase = {
    KamanjaStatusEvent.build(this)
  }
  
  
    override def hasPrimaryKey(): Boolean = {
    	KamanjaStatusEvent.hasPrimaryKey;
    }

  override def hasPartitionKey(): Boolean = {
    KamanjaStatusEvent.hasPartitionKey;
  }

  override def hasTimeParitionInfo(): Boolean = {
    KamanjaStatusEvent.hasTimeParitionInfo;
  }
  
}