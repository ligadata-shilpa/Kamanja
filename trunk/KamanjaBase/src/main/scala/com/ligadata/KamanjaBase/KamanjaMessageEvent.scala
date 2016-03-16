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

object KamanjaMessageEvent extends RDDObject[KamanjaMessageEvent] with BaseMsgObj {
	override def TransformDataAttributes: TransformMessage = null
	override def NeedToTransformData: Boolean = false
	override def FullName: String = "com.ligadata.KamanjaBase.KamanjaMessageEvent"
	override def NameSpace: String = "com.ligadata.KamanjaBase"
	override def Name: String = "KamanjaMessageEvent"
	override def Version: String = "000001.000003.000000"
	override def CreateNewMessage: BaseMsg  = new KamanjaMessageEvent()
	override def IsFixed:Boolean = true;
	override def IsKv:Boolean = false;
	 override def CanPersist: Boolean = false;

 
  type T = KamanjaMessageEvent     
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

class KamanjaMessageEvent(var transactionId: Long, other: KamanjaMessageEvent) extends BaseMsg {
	override def IsFixed : Boolean = KamanjaMessageEvent.IsFixed;
	override def IsKv : Boolean = KamanjaMessageEvent.IsKv;

	 override def CanPersist: Boolean = KamanjaMessageEvent.CanPersist;

	override def FullName: String = KamanjaMessageEvent.FullName
	override def NameSpace: String = KamanjaMessageEvent.NameSpace
	override def Name: String = KamanjaMessageEvent.Name
	override def Version: String = KamanjaMessageEvent.Version


	var messageid:Long = _ ;
	var modelinfo: scala.collection.mutable.ArrayBuffer[com.ligadata.KamanjaBase.KamanjaModelEvent] = new scala.collection.mutable.ArrayBuffer[com.ligadata.KamanjaBase.KamanjaModelEvent];
	var elapsedtimeinms:Int = _ ;
	var error:String = _ ;

	override def toString: String = {
		var rs = "Message \n"
		rs = rs + messageid.toString + ", \n"
		rs = rs + "took " + elapsedtimeinms + "MS, \n "
		rs = rs + "To Execute these models\n"
		modelinfo.foreach(x=> {
			rs = rs + "id: " + x.modelid + "\n"
			rs = rs + " took " + x.elapsedtimeinms + "MS and returned a result " + x.isresultproduced
		})
		rs = rs + "\n" + "--------"

		return rs
	}

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
	    	 if(key.equals("messageid")) return messageid; 
	 if(key.equals("modelinfo")) return modelinfo; 
	 if(key.equals("elapsedtimeinms")) return elapsedtimeinms; 
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
		  val fieldX = ru.typeOf[KamanjaMessageEvent].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
	  val fmX = im.reflectField(fieldX)
	  fmX.get
	}
    
    private val LOG = LogManager.getLogger(getClass)
    
    override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = { }
     
     
    override def GetMessage(childPath: Array[(String, String)], primaryKey:Array[String]): com.ligadata.KamanjaBase.BaseMsg = {
       return null
    } 
     
     
    override def Save: Unit = {
		 KamanjaMessageEvent.saveOne(this)
	}
 	 
    var nativeKeyMap  =  scala.collection.mutable.Map[String, String](("messageid", "messageId"), ("modelinfo", "ModelInfo"), ("elapsedtimeinms", "ElapsedTimeInMs"), ("error", "Error"))
    
    override def getNativeKeyValues(): scala.collection.immutable.Map[String, (String, Any)] = {

    var keyValues: scala.collection.mutable.Map[String, (String, Any)] = scala.collection.mutable.Map[String, (String, Any)]()

    try {
         	 keyValues("messageid") = ("messageId", messageid); 
	 keyValues("modelinfo") = ("ModelInfo", modelinfo); 
	 keyValues("elapsedtimeinms") = ("ElapsedTimeInMs", elapsedtimeinms); 
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
			if(list.size < 4) throw new Exception("Incorrect input data size")
	  		messageid = com.ligadata.BaseTypes.LongImpl.Input(list(inputdata.curPos));
		inputdata.curPos = inputdata.curPos+1;
		elapsedtimeinms = com.ligadata.BaseTypes.IntImpl.Input(list(inputdata.curPos));
		inputdata.curPos = inputdata.curPos+1;
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
    
	  		 messageid = com.ligadata.BaseTypes.LongImpl.Input(map.getOrElse("messageid", 0).toString);
		 elapsedtimeinms = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("elapsedtimeinms", 0).toString);
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
			val _messageidval_  = (xml \\ "messageid").text.toString 
			if (_messageidval_  != "")		messageid =  com.ligadata.BaseTypes.LongImpl.Input( _messageidval_ ) else messageid = 0;
			val _elapsedtimeinmsval_  = (xml \\ "elapsedtimeinms").text.toString 
			if (_elapsedtimeinmsval_  != "")		elapsedtimeinms =  com.ligadata.BaseTypes.IntImpl.Input( _elapsedtimeinmsval_ ) else elapsedtimeinms = 0;
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
	 
	  		 messageid = com.ligadata.BaseTypes.LongImpl.Input(map.getOrElse("messageid", 0).toString);
		 elapsedtimeinms = com.ligadata.BaseTypes.IntImpl.Input(map.getOrElse("elapsedtimeinms", 0).toString);
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
    	   	com.ligadata.BaseTypes.LongImpl.SerializeIntoDataOutputStream(dos,messageid);
		if ((modelinfo==null) ||(modelinfo.size == 0)) com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,0);
		else {
		com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,modelinfo.size);
		modelinfo.foreach(obj => {
		val bytes = SerializeDeserialize.Serialize(obj)
		com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,bytes.length)
		dos.write(bytes)})}
	com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos,elapsedtimeinms);
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
              	messageid = com.ligadata.BaseTypes.LongImpl.DeserializeFromDataInputStream(dis);
	{
			var arraySize = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
		val i:Int = 0;
		 for (i <- 0 until arraySize) {
		var bytes = new Array[Byte](com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis))
		dis.read(bytes);
		val inst = SerializeDeserialize.Deserialize(bytes, mdResolver, loader, false, "com.ligadata.KamanjaBase.KamanjaModelEvent");
		modelinfo += inst.asInstanceOf[com.ligadata.KamanjaBase.KamanjaModelEvent];
		}

		}
	elapsedtimeinms = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis);
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
   
	 def withmessageid(value: Long) : KamanjaMessageEvent = {
	 this.messageid = value 
	 return this 
 	 } 

 	 def withmodelinfo(value: scala.collection.mutable.ArrayBuffer[com.ligadata.KamanjaBase.KamanjaModelEvent]) : KamanjaMessageEvent = {
	 this.modelinfo = value 
	 return this 
 	 } 

	 def withelapsedtimeinms(value: Int) : KamanjaMessageEvent = {
	 this.elapsedtimeinms = value 
	 return this 
 	 } 

	 def witherror(value: String) : KamanjaMessageEvent = {
	 this.error = value 
	 return this 
 	 } 
 
     private def fromFunc(other: KamanjaMessageEvent): KamanjaMessageEvent = {
     		messageid = com.ligadata.BaseTypes.LongImpl.Clone(other.messageid);
		elapsedtimeinms = com.ligadata.BaseTypes.IntImpl.Clone(other.elapsedtimeinms);
		error = com.ligadata.BaseTypes.StringImpl.Clone(other.error);

			 if (other.modelinfo != null ) { 
		 modelinfo.clear;  
		 other.modelinfo.map(v =>{ modelinfo :+= v.Clone.asInstanceOf[com.ligadata.KamanjaBase.KamanjaModelEvent]}); 
		 } 
		 else modelinfo = null; 

	timePartitionData = com.ligadata.BaseTypes.LongImpl.Clone(other.timePartitionData);
    return this
    }
    
      def ComputeTimePartitionData: Long = {
		val tmPartInfo = KamanjaMessageEvent.getTimePartitionInfo
		if (tmPartInfo == null) return 0;
		KamanjaMessageEvent.ComputeTimePartitionData(" ", tmPartInfo._2, tmPartInfo._3)
	 } 
  
    def getTimePartition() : KamanjaMessageEvent = {
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
  def this(other: KamanjaMessageEvent) = {
    this(0, other)
  }
  def this() = {
    this(0, null)
  }
  override def Clone(): MessageContainerBase = {
    KamanjaMessageEvent.build(this)
  }
  
  
    override def hasPrimaryKey(): Boolean = {
    	KamanjaMessageEvent.hasPrimaryKey;
    }

  override def hasPartitionKey(): Boolean = {
    KamanjaMessageEvent.hasPartitionKey;
  }

  override def hasTimeParitionInfo(): Boolean = {
    KamanjaMessageEvent.hasTimeParitionInfo;
  }
  
}