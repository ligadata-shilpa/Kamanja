package com.ligadata.queryutility

import java.util.Properties
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.sql.ResultSet

import com.ligadata.kamanja.metadata.{AdapterInfo, ContainerDef, MessageDef, ModelDef};
/**
  * Created by Yousef on 6/16/2016.
  */
class QueryBuilder extends LogTrait {

  def createQuery(setQuery: String, elementType: String, className: String , linkFrom: Option[String] = None, linkTo: Option[String] = None,
                  extendsClass: Option[String] = None): String = {
    var query: String = ""
    if(elementType.equalsIgnoreCase("vertex")){
      query = "create vertex %s %s;".format(className,setQuery)
    } else if(elementType.equalsIgnoreCase("edge")){
      query= "create edge %s from %s to %s %s;".format(className,linkFrom.get,linkTo.get, setQuery)
//    } else if(elementType.equalsIgnoreCase("class")){
//      query = "create class %s extends %s;".format(setQuery, extendsClass)
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

  def createSetCommand(message: Option[MessageDef]= None, contianer: Option[ContainerDef]= None, model: Option[ModelDef]= None, adapter: Option[AdapterInfo] = None): String ={
    var setQuery: String = ""

    if(message != None){
      setQuery = "set ID = %i, Name = \"%s\", Namespace = \"%s\", FullName = \"%s\", Version = \"%s\", CreatedBy = \"%s\", CreatedTime = \"%s\",".format(
        message.get.UniqId, message.get.Name, message.get.NameSpace, message.get.FullName, message.get.Version, message.get.CreationTime) +
        " Tenant = %s, Description = \"%s\", Author = \"%s\", Active = %b, Type = \'V\'".format(
          message.get.TenantId.toString, message.get.Description, message.get.Author, message.get.Active)
    } else if(contianer != None){
      setQuery = "set ID = %i, Name = \"%s\", Namespace = \"%s\", FullName = \"%s\", Version = \"%s\", CreatedBy = \"%s\", CreatedTime = \"%s\", ".format(
        contianer.get.UniqId, contianer.get.Name, contianer.get.NameSpace, contianer.get.FullName, contianer.get.Version, contianer.get.CreationTime) +
        " Tenant = %s, Description = \"%s\", Author = \"%s\", Active = %b, Type = \'V\'".format(
          contianer.get.TenantId.toString, contianer.get.Description, contianer.get.Author, contianer.get.Active)
    } else if (model != None){
      setQuery = "set ID = %i, Name = \"%s\", Namespace = \"%s\", FullName = \"%s\", Version = \"%s\", CreatedBy = \"%s\", CreatedTime = \"%s\", ".format(
        model.get.UniqId, model.get.Name, model.get.NameSpace, model.get.FullName, model.get.Version, model.get.CreationTime) +
        " Tenant = %s, Description = \"%s\", Author = \"%s\", Active = %b, Type = \'V\'".format(
          model.get.TenantId.toString, model.get.Description, model.get.Author, model.get.Active)
    } else if(adapter != None){
      setQuery = "set Name = \"%s\", Tenant = \"%s\", Type = \'V\'".format(adapter.get.Name, adapter.get.TenantId.toString)
    }
    return setQuery
  }

}
