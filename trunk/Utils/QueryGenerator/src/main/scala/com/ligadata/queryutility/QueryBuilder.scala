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
    } else if(elementType.equalsIgnoreCase("class")){
      query = "create class %s extends %s;".format(setQuery, extendsClass)
    }
    return query
  }

  def checkQuery(elementType: String, objName: String ,className: String , linkFrom: Option[String] = None, linkTo: Option[String] = None): String ={
    var query: String = ""
    if(elementType.equalsIgnoreCase("vertex")){
      query = "select * from %s where Name = \"%s\";".format(className,objName)
//    } else if(elementType.equalsIgnoreCase("edge")){
//      query= "select * from %s and Name = \"%s\" and in = (select @rid from V where Name = \"%s\");".format(className,objName,linkFrom.get,linkTo.get)
    } else if(elementType.equalsIgnoreCase("class")){
      query = "select distinct(@class) from V where @class = \"%s\";".format(className)
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

  def checkObjexsist(conn: Connection, query: String): Boolean ={
    var stmt: Statement = conn.createStatement()
    var result : ResultSet = stmt.executeQuery(query)
    if(result.next())
      return true // there is data
    else
      return false // there is no data
  }
  def createSetCommand(message: Option[MessageDef]= None, contianer: Option[ContainerDef]= None, model: Option[ModelDef]= None, adapter: Option[AdapterInfo] = None): String ={
    var setQuery: String = ""

    if(message != None){
      val tenantID: String =
        if(message.get.TenantId.isEmpty) ""
        else message.get.TenantId

      setQuery = "set ID = " + message.get.UniqId + ", Name = \"%s\", Namespace = \"%s\", FullName = \"%s\", Version = \"%s\",  CreatedTime = \"%s\",".format(
         message.get.Name, message.get.NameSpace, message.get.FullName, message.get.Version, message.get.CreationTime) +
        " Tenant = %s, Description = \"%s\", Author = \"%s\", Active = %b, Type = \'V\'".format(
          tenantID, message.get.Description, message.get.Author, message.get.Active)
    } else if(contianer != None){
        val tenantID: String =
          if(contianer.get.TenantId.isEmpty) ""
          else contianer.get.TenantId

      setQuery = "set ID = "+ contianer.get.UniqId + ", Name = \"%s\", Namespace = \"%s\", FullName = \"%s\", Version = \"%s\", CreatedTime = \"%s\", ".format(
         contianer.get.Name, contianer.get.NameSpace, contianer.get.FullName, contianer.get.Version, contianer.get.CreationTime) +
        " Tenant = %s, Description = \"%s\", Author = \"%s\", Active = %b, Type = \'V\'".format(
          tenantID, contianer.get.Description, contianer.get.Author, contianer.get.Active)
    } else if (model != None){
      val tenantID: String =
        if(model.get.TenantId.isEmpty) ""
        else model.get.TenantId

      setQuery = "set ID = " + model.get.UniqId + ", Name = \"%s\", Namespace = \"%s\", FullName = \"%s\", Version = \"%s\", CreatedTime = \"%s\", ".format(
         model.get.Name, model.get.NameSpace, model.get.FullName, model.get.Version, model.get.CreationTime) +
        " Tenant = %s, Description = \"%s\", Author = \"%s\", Active = %b, Type = \'V\'".format(
          tenantID, model.get.Description, model.get.Author, model.get.Active)
    } else if(adapter != None){
      setQuery = "set Name = \"%s\", Tenant = \"%s\", Type = \'V\'".format(adapter.get.Name, adapter.get.TenantId.toString)
    }
    return setQuery
  }

}
