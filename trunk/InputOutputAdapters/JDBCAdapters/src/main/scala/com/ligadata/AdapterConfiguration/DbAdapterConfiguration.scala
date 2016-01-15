package com.ligadata.AdaptersConfiguration

import com.ligadata.InputOutputAdapterInfo.{ AdapterConfiguration, PartitionUniqueRecordKey, PartitionUniqueRecordValue }
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import java.util.Date
import org.apache.logging.log4j.LogManager
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.sql.DatabaseMetaData
import java.sql.ResultSet

class DbAdapterConfiguration extends AdapterConfiguration {
    var dbDriver : String = _ //Name of the driver class
    var dbName : String = _ //Name of the DB
    var dbUser : String = _ //Db User Name for connectivity
    var dbPwd : String = _ //Db User Password for connectivity
    var dbURL : String = _ //JDBC Connectivity URL
    
    var query : String = _ //Query to pull data
    
    var table : String = _ //Table Name
    var columns : String = _ //Column Names for Order and Select 
    var where : String = _ //Where clause for table Name
    
    var temporalColumnName : String = _ //Temporal Column for running as a Job/Continuously
    var pkColumnName : String = _ //Primary Key column Name (for uniqueness of each message)
    
    var timeInterval : Long = 0 //0 or -1 means run once, any greater value, will run continuously
    var timeUnits : String = _ //Valid units are SECONDS, MINUTES, HOURS, DAYS, MONTHS, YEARS (values from java TimeUnit)
    
    //var MessagePrefix: String = _ // This is the first String in the message
    //var AddTS2MsgFlag: Boolean = false // Add TS after the Prefix Msg
}

object DbAdapterConfiguration {
  private[this] val LOG = LogManager.getLogger(getClass);
  
  def getAdapterConfig(inputConfig: AdapterConfiguration):DbAdapterConfiguration = {
    if (inputConfig.adapterSpecificCfg == null || inputConfig.adapterSpecificCfg.size == 0) {
      val err = "Not found Db Adapter Config:" + inputConfig.Name
      throw new Exception(err)
    }
    
    val dbAdpt = new DbAdapterConfiguration
    
    dbAdpt.Name = inputConfig.Name
    dbAdpt.formatOrInputAdapterName = inputConfig.formatOrInputAdapterName
    dbAdpt.className = inputConfig.className
    dbAdpt.jarName = inputConfig.jarName
    dbAdpt.dependencyJars = inputConfig.dependencyJars
    dbAdpt.associatedMsg = if (inputConfig.associatedMsg != null) null else inputConfig.associatedMsg.trim
    dbAdpt.keyAndValueDelimiter = if (inputConfig.keyAndValueDelimiter == null) null else inputConfig.keyAndValueDelimiter.trim
    dbAdpt.fieldDelimiter = if (inputConfig.fieldDelimiter == null) null else inputConfig.fieldDelimiter.trim
    dbAdpt.valueDelimiter = if (inputConfig.valueDelimiter == null) null else inputConfig.valueDelimiter.trim
    
    val adapCfg = parse(inputConfig.adapterSpecificCfg)
    if (adapCfg == null || adapCfg.values == null) {
      val err = "Not found DB Adapter Config:" + inputConfig.Name
      throw new Exception(err)
    }
    val values = adapCfg.values.asInstanceOf[Map[String, String]]
    
     values.foreach(kv => {
      if (kv._1.compareToIgnoreCase("dbDriver") == 0) {
        dbAdpt.dbDriver = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("dbName") == 0) {
        dbAdpt.dbName = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("dbUser") == 0) {
        dbAdpt.dbUser = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("dbPwd") == 0) {
        dbAdpt.dbPwd = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("dbURL") == 0) {
        dbAdpt.dbURL = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("query") == 0) {
        dbAdpt.query = kv._2.trim
      }else if (kv._1.compareToIgnoreCase("table") == 0) {
        dbAdpt.table = kv._2.trim
      }else if (kv._1.compareToIgnoreCase("columns") == 0) {
        dbAdpt.columns = kv._2.trim
      }else if (kv._1.compareToIgnoreCase("where") == 0) {
        dbAdpt.where = kv._2.trim
      }else if (kv._1.compareToIgnoreCase("temporalColumnName") == 0) {
        dbAdpt.temporalColumnName = kv._2.trim
      }else if (kv._1.compareToIgnoreCase("pkColumnName") == 0) {
        dbAdpt.pkColumnName = kv._2.trim
      }else if (kv._1.compareToIgnoreCase("timeInterval") == 0) {
        dbAdpt.timeInterval = kv._2.trim.toLong
      }else if (kv._1.compareToIgnoreCase("timeUnits") == 0) {
        dbAdpt.timeUnits = kv._2.trim
      }/*else if (kv._1.compareToIgnoreCase("MessagePrefix") == 0) {
        dbAdpt.MessagePrefix = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("AddTS2MsgFlag") == 0) {
        dbAdpt.AddTS2MsgFlag = kv._2.trim.toBoolean
      }*/
    })
    
    //Validate DB Connectivity Parameters
    if(dbAdpt.dbDriver !=null && !dbAdpt.dbDriver.isEmpty()){
      try{
        Class.forName(dbAdpt.dbDriver)
        
        var connection:Connection = null
        
        //Set 10 sec timeout for DriverManager to fetch connection
        DriverManager.setLoginTimeout(10)
        
        if(dbAdpt.dbName != null && !dbAdpt.dbName.isEmpty())
          connection = DriverManager.getConnection(dbAdpt.dbURL+"/"+dbAdpt.dbName, dbAdpt.dbUser, dbAdpt.dbPwd)
        else
          connection = DriverManager.getConnection(dbAdpt.dbURL, dbAdpt.dbUser, dbAdpt.dbPwd)
        
        var statement:Statement = connection.createStatement()
        
        //Validate Table Details
        var dbmd: DatabaseMetaData = connection.getMetaData
        var rs:ResultSet = null;
        
        //Validate Table details
        if(dbAdpt.table != null && !dbAdpt.table.isEmpty()){
          rs = dbmd.getTables(null, null, dbAdpt.table, Array("TABLE"))
          if(rs != null && rs.next()){
          }else
            LOG.error("Error while fetching table details..")
        
        //Validate the Query
        }else if(dbAdpt.query != null && !dbAdpt.query.isEmpty()){
          statement.execute(dbAdpt.query.concat(" LIMIT 1"))
          rs = statement.getResultSet
          if(rs != null && rs.next()){
          }else
            LOG.error("Error while fetching query results..")
        
        //Neither Query nor Table Specified, log error
        }else{
          LOG.error("Neither query nor table name specified")
        } 
        
        //Close the resultset
        if(rs != null){
          rs.close()
          rs = null
        }
        
        //Close the Statement
        if(statement != null){
          statement.close()
          statement = null
        }
        //Finally Close the connection  
        if(connection != null){
          connection.close
          connection = null
        }
          
      }catch{
        case exc:ClassNotFoundException =>  LOG.error("Unable to find the JDBC Driver in the classpath..".concat(exc.getMessage))
        case exc:SQLException => LOG.error("Error while validating DB Adapter parameters..".concat(exc.getMessage))
      }
    }else{
      LOG.error("JDBC Driver Classname not specified or empty.")
    }
    dbAdpt
  }
}

case class DbKeyData(Version: Int, Type: String, DBUrl: String, DBName: String, Query: Option[String], TableName: Option[String], Columns:Option[String], WhereClause: Option[String]) 

class DbPartitionUniqueRecordKey extends PartitionUniqueRecordKey {
    val Version: Int = 1
    val Type: String = "Database"
    var DBUrl: String = _
    var DBName: String = _
    var Query: String = _
    var TableName: String = _
    var Columns: String = _
    var WhereClause: String = _
    
    override def Serialize: String = { // Making String from key
      val json =
        ("Version" -> Version) ~
          ("Type" -> Type) ~
          ("DBUrl" -> DBUrl) ~
          ("DBName" -> DBName) ~
          ("Query" -> Query) ~
          ("TableName" -> TableName) ~
          ("Columns" -> Columns) ~
          ("WhereClause" -> WhereClause)
        compact(render(json))
    }
    
    override def Deserialize(key: String): Unit = { // Making Key from Serialized String
      implicit val jsonFormats: Formats = DefaultFormats
      val keyData = parse(key).extract[DbKeyData]
      if (keyData.Version == Version && keyData.Type.compareTo(Type) == 0) {
        DBUrl = keyData.DBUrl
        DBName = keyData.DBName
        Query = keyData.Query.get
        TableName = keyData.TableName.get
        Columns = keyData.Columns.get
        WhereClause = keyData.WhereClause.get
      }
      // else { } // Not yet handling other versions
  }
}

case class DbRecData(Version: Int, PrimaryKeyValue: Option[String], AddedDate: Option[Date])

class DbPartitionUniqueRecordValue extends PartitionUniqueRecordValue {
  val Version: Int = 1
  var PrimaryKeyValue: String = _ //Primary Key Column Value for the ROW
  var AddedDate: Date = _ //Temporal Column Value
  
  override def Serialize: String = { // Making String from Value
    val json =
      ("Version" -> Version) ~
        ("PrimaryKeyValue" -> PrimaryKeyValue) ~
        //TODO Check if Date toString works in SERDE
        ("AddedDate" -> AddedDate.getTime)
    compact(render(json))
  }

  override def Deserialize(key: String): Unit = { // Making Value from Serialized String
    implicit val jsonFormats: Formats = DefaultFormats
    val recData = parse(key).extract[DbRecData]
    if (recData.Version == Version) {
      PrimaryKeyValue = recData.PrimaryKeyValue.get
      AddedDate = recData.AddedDate.get
    }
    // else { } // Not yet handling other versions
  }
  
}