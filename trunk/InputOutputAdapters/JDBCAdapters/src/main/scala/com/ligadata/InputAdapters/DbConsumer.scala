

package com.ligadata.InputAdapters

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Statement
import java.util.Date
import scala.actors.threadpool.{ Executors, ExecutorService }
import scala.util.control.Breaks.{break, breakable}
import org.apache.logging.log4j.LogManager
import com.ligadata.AdaptersConfiguration.{DbAdapterConfiguration, DbPartitionUniqueRecordKey,DbPartitionUniqueRecordValue}
import com.ligadata.InputOutputAdapterInfo.AdapterConfiguration
import com.ligadata.InputOutputAdapterInfo.CountersAdapter
import com.ligadata.InputOutputAdapterInfo.ExecContextObj
import com.ligadata.InputOutputAdapterInfo.InputAdapter
import com.ligadata.InputOutputAdapterInfo.InputAdapterCallerContext
import com.ligadata.InputOutputAdapterInfo.InputAdapterObj
import com.ligadata.InputOutputAdapterInfo.PartitionUniqueRecordKey
import com.ligadata.InputOutputAdapterInfo.PartitionUniqueRecordValue
import com.ligadata.InputOutputAdapterInfo.StartProcPartInfo
import com.ligadata.KamanjaBase.DataDelimiters
import javax.sql.DataSource
import org.apache.commons.dbcp2.BasicDataSource


object DbConsumer extends InputAdapterObj {
  def CreateInputAdapter(inputConfig: AdapterConfiguration, callerCtxt: InputAdapterCallerContext, execCtxtObj: ExecContextObj, cntrAdapter: CountersAdapter): InputAdapter = new DbConsumer(inputConfig, callerCtxt, execCtxtObj, cntrAdapter)
}

class DbConsumer (val inputConfig: AdapterConfiguration, val callerCtxt: InputAdapterCallerContext, val execCtxtObj: ExecContextObj, cntrAdapter: CountersAdapter) extends InputAdapter {
  private[this] val LOG = LogManager.getLogger(getClass);
  private[this] val dcConf = DbAdapterConfiguration.getAdapterConfig(inputConfig)
  private[this] val lock = new Object()
  
  private[this] var uniqueKey: DbPartitionUniqueRecordKey = new DbPartitionUniqueRecordKey
  uniqueKey.DBUrl = dcConf.dbURL
  uniqueKey.DBName = dcConf.dbName
  uniqueKey.TableName = dcConf.table
  uniqueKey.WhereClause = dcConf.where
  uniqueKey.Columns = dcConf.columns
  uniqueKey.Query = dcConf.query
  
  //DataSource for the connection Pool
  private[this] var dataSource: BasicDataSource = _
  
  var executor: ExecutorService = _
  val input = this
   
  val execThread = execCtxtObj.CreateExecContext(input, uniqueKey, callerCtxt)
  
  override def Shutdown: Unit = lock.synchronized {
    StopProcessing
  }
  
  override def StopProcessing: Unit = lock.synchronized {
    LOG.debug("Initiating Stop Processing...")
    
    //Shutdown the executor
    if (executor != null){
      executor.shutdownNow
      while (executor.isTerminated == false) {
        Thread.sleep(100) // sleep 100ms and then check
      }
      executor = null
    }else return
    
    
    if(dataSource != null){
      dataSource.close
      dataSource = null
    }else return
    
  }
  
  override def StartProcessing(partitionInfo: Array[StartProcPartInfo], ignoreFirstMsg: Boolean): Unit = lock.synchronized {
    if (partitionInfo == null || partitionInfo.size == 0)
      return
    
    val delimiters = new DataDelimiters
    delimiters.keyAndValueDelimiter = dcConf.keyAndValueDelimiter
    delimiters.fieldDelimiter = dcConf.fieldDelimiter
    delimiters.valueDelimiter = dcConf.valueDelimiter
    
    var threads: Int = 1
    
    //TODO Need to see if we can run a thread pool with timeInterval and timeUnits 
    executor = Executors.newFixedThreadPool(threads)
    
    //Create a DBCP based Connection Pool Here
    dataSource = new BasicDataSource
		
		dataSource.setDriverClassName(dcConf.dbDriver)
		if(dcConf.dbName!=null && !dcConf.dbName.isEmpty())
		  dataSource.setUrl(dcConf.dbURL+"/"+dcConf.dbName)
		else
		  dataSource.setUrl(dcConf.dbURL)
		dataSource.setUsername(dcConf.dbUser);
		dataSource.setPassword(dcConf.dbPwd);
		
		
		dataSource.setTestWhileIdle(false);
		dataSource.setTestOnBorrow(true);
		dataSource.setValidationQuery("Select 1");
		dataSource.setTestOnReturn(false);
		
		dataSource.setMaxTotal(100);
		dataSource.setMaxIdle(5);
		dataSource.setMinIdle(0);
		dataSource.setInitialSize(5);
		dataSource.setMaxWaitMillis(5000);
    
    val uniqueValue = new DbPartitionUniqueRecordValue
    
    //Record the last run time on this counter value
    val runIntervalKey:String = Category concat "/" concat  dcConf.Name concat dcConf.dbName
    val lastRun:Long = cntrAdapter.getCntr(runIntervalKey);
    
    try{
      executor.execute(new Runnable() {
        override def run() {
          
          var connection:Connection = null
          var statement:Statement = null
          var preparedStatement: PreparedStatement = null
          var resultset:ResultSet = null
          var resultSetMetaData: ResultSetMetaData = null
          
          connection = dataSource.getConnection
          statement = connection.createStatement
          
          //TODO Dynamic insertion of where clause to include the 
          if(dcConf.query != null && !dcConf.query.isEmpty()){
             statement.execute(dcConf.query)
          }else{
            if(dcConf.where != null && !dcConf.where.isEmpty()){
              statement.execute("Select ".concat(dcConf.columns)
                   .concat(" from ").concat(dcConf.table)
                   .concat(" ").concat(dcConf.where))
            }else{
              statement.execute("Select ".concat(dcConf.columns)
                   .concat(" from ").concat(dcConf.table))
            }  
          }
          resultset = statement.getResultSet
          resultSetMetaData = resultset.getMetaData
          
          var cntr: Long = 0
          
          breakable{
            
            while(resultset.next){
              val readTmNs = System.nanoTime
              val readTmMs = System.currentTimeMillis
              
              val sbf:StringBuffer = new StringBuffer
              var cols:Int = 0
              for(cols <- 1 until resultSetMetaData.getColumnCount){
                 if(resultSetMetaData.getColumnName(cols).equalsIgnoreCase(dcConf.pkColumnName))
                   uniqueValue.PrimaryKeyValue = resultset.getObject(cols).toString()
                 
                 if(resultSetMetaData.getColumnName(cols).equalsIgnoreCase(dcConf.temporalColumnName))
                   uniqueValue.AddedDate = resultset.getDate(cols)
                  
                 sbf.append(resultset.getObject(cols).toString())
                 if(cols != resultSetMetaData.getColumnCount)
                   sbf.append(dcConf.fieldDelimiter)
              }
              
              execThread.execute(sbf.toString().getBytes, dcConf.formatOrInputAdapterName, uniqueKey, uniqueValue, readTmNs, readTmMs, false, dcConf.associatedMsg, delimiters)
                          
              cntr += 1
              val key = Category concat "/" concat dcConf.Name concat "/evtCnt"
              cntrAdapter.addCntr(key, 1) // for now adding each row
              
              
               if (executor.isShutdown) {
                 break
               }
            }
            
            //Capture the last run time here and push into a counter
            val currentTime:Long = new Date().getTime;
            if(lastRun == 0)
              cntrAdapter.addCntr(runIntervalKey, currentTime)
            else 
              cntrAdapter.addCntr(runIntervalKey, (currentTime - lastRun))
          }
          //breakable ends here
          
          try{
            if(resultset != null){
               resultset.close
              resultset = null
            }
            if(statement != null){
              statement.close
              statement = null
            }
            if(preparedStatement != null){
              preparedStatement.close
              preparedStatement = null
            }
            if(connection != null){
              connection.close
              connection = null
            }
          }catch{
            case exc:SQLException =>  LOG.error("Error while closing resources ".concat(exc.getMessage))
          }finally{
            try{
               if(resultset != null){
                  resultset.close
                  resultset = null
                }
                if(statement != null){
                  statement.close
                  statement = null
                }
                if(preparedStatement != null){
                  preparedStatement.close
                  preparedStatement = null
                }
                if(connection != null){
                  connection.close
                  connection = null
                }
            }catch{
              case exc:SQLException =>  LOG.error("Error while closing resources ".concat(exc.getMessage))
            }
          }
          
        }
      })
    } catch {
      case e: Exception => {
        LOG.error("Failed to setup Streams. Reason:%s Message:%s".format(e.getCause, e.getMessage))
      }
    }
  }
  
  override def GetAllPartitionUniqueRecordKey: Array[PartitionUniqueRecordKey] = lock.synchronized {
    GetAllPartitionsUniqueKeys
  }
  
  private def GetAllPartitionsUniqueKeys: Array[PartitionUniqueRecordKey] = lock.synchronized {
    val uniqueKey = new DbPartitionUniqueRecordKey
      uniqueKey.DBName = dcConf.dbName
      uniqueKey.DBUrl = dcConf.dbURL
      uniqueKey.TableName = dcConf.table
      uniqueKey.Query = dcConf.query
      uniqueKey.Columns = dcConf.columns
      uniqueKey.WhereClause = dcConf.where
    Array[PartitionUniqueRecordKey](uniqueKey)
  }
  
  override def DeserializeKey(k: String): PartitionUniqueRecordKey = {
    val key = new DbPartitionUniqueRecordKey
    try {
      LOG.debug("Deserializing Key:" + k)
      key.Deserialize(k)
    } catch {
      case e: Exception => {
        LOG.error("Failed to deserialize Key:%s. Reason:%s Message:%s".format(k, e.getCause, e.getMessage))
        throw e
      }
    }
    key
  }

  override def DeserializeValue(v: String): PartitionUniqueRecordValue = {
    val vl = new DbPartitionUniqueRecordValue
    if (v != null) {
      try {
        LOG.debug("Deserializing Value:" + v)
        vl.Deserialize(v)
      } catch {
        case e: Exception => {
          LOG.error("Failed to deserialize Value:%s. Reason:%s Message:%s".format(v, e.getCause, e.getMessage))
          throw e
        }
      }
    }
    vl
  }
  
  // Not yet implemented
  override def getAllPartitionBeginValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = {
    return Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]()
  }

  // Not yet implemented
  override def getAllPartitionEndValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = {
    return Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]()
  }
  
}