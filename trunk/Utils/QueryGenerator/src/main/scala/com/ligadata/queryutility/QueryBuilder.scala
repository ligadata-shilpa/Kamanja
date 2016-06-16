package com.ligadata.queryutility

import java.util.Properties
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.sql.ResultSet;
/**
  * Created by Yousef on 6/16/2016.
  */
class QueryBuilder extends LogTrait {

  def createQuery(elementName: String, elementType: String, className: String , linkFrom: Option[String] = None, linkTo: Option[String] = None): String = {
    var query: String = ""
    if(elementType.equalsIgnoreCase("vertex")){
      query = "create %s class %s set Name = \"%s\";".format(elementType,className,elementName)
    } else if(elementName.equalsIgnoreCase("edge")){
      query= "create %s %s from %s to %s set Name = \"%s\";".format(elementType,className,linkFrom.get,linkTo.get, elementName)
    }
    return query
  }

  def getDBConnection(configObj: ConfigBean): Connection ={
    Class.forName("com.orientechnologies.orient.jdbc.OrientJdbcDriver")
    var info: Properties = new Properties
    info.put("user", configObj.username);  //username==> form configfile
    info.put("password", configObj.password); //password ==> from configfile
    info.put("db.usePool", "true"); // USE THE POOL
    info.put("db.pool.min", "1");   // MINIMUM POOL SIZE
    info.put("db.pool.max", "16");  // MAXIMUM POOL SIZE
    val conn = DriverManager.getConnection(configObj.url, info); // url==> jdbc:orient:remote:localhost/test"

    return conn
  }

  def executeQuery(conn: Connection, query: String): Unit ={
    var stmt: Statement = conn.createStatement()
    stmt.execute(query)
    stmt.close()
    // should add try catch exception and add it to log
  }

}
