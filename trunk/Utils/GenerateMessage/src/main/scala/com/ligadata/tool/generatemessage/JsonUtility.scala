package com.ligadata.tool.generatemessage

/**
  * Created by Yousef on 5/16/2016.
  */
import org.json4s
import org.json4s.JsonAST
import org.json4s.JsonAST.JInt
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import scala.collection.immutable.Map

class JsonUtility  extends LogTrait {
  def CreateMainJsonString(data: Map[String, String], configObj: ConfigBean): JsonAST.JValue = {
    val json =
      ("Meesage" ->
        ("NameSpace" -> configObj.nameSpace) ~
          ("Name" -> configObj.messageName) ~
          ("Verion" -> "00.01.00") ~
          ("Description" -> "") ~
          ("Fixed" -> configObj.messageStructure.toString) ~
          ("Persist" -> configObj.saveMessage.toString) ~
          ("Feilds" ->
            data.keys.map {
              key =>
                (
                  ("Name" -> key) ~
                    ("Type" -> "System.".concat(data(key))))
            })
        )
    return json
  }

  def CreateJsonString(feild: String, configObj: ConfigBean, keys: Array[String]): JsonAST.JValue = {
    var json: JsonAST.JValue = ""
    if (!feild.equalsIgnoreCase("TimePartitionInfo")) {
      json = ("Meesage" ->
        ("NameSpace" -> configObj.nameSpace) ~
          ("Name" -> configObj.messageName) ~
          ("Verion" -> "00.01.00") ~
          ("Description" -> "") ~
          ("Fixed" -> configObj.messageStructure.toString) ~
          ("Persist" -> configObj.saveMessage.toString) ~
          (feild -> /*List.empty[JInt]*/ keys.toList)
        )
    } else {
      json = ("Meesage" ->
        ("NameSpace" -> configObj.nameSpace) ~
          ("Name" -> configObj.messageName) ~
          ("Verion" -> "00.01.00") ~
          ("Description" -> "") ~
          ("Fixed" -> configObj.messageStructure.toString) ~
          ("Persist" -> configObj.saveMessage.toString) ~
          (feild -> ("Key" -> configObj.timePartition) ~
            ("Format" -> "epochtime") ~
            ("Type" -> "Daily"))
        )
    }
    return json
  }

  def FinalJsonString(data: Map[String, String], configObj: ConfigBean): JsonAST.JValue = {
    var json: JsonAST.JValue = ""
    if (!data.isEmpty) {
      json = CreateMainJsonString(data, configObj)
      if (configObj.hasPartitionKey == true) {
        val jsonPatitionKey = CreateJsonString("PartitionKey", configObj, configObj.partitionKeyArray)
        json = json merge jsonPatitionKey
      }

      if (configObj.hasPrimaryKey == true) {
        val jsonPrimaryKey = CreateJsonString("PrimaryKey", configObj, configObj.primaryKeyArray)
        json = json merge jsonPrimaryKey
      }

      if (configObj.hasTimePartition == true) {
        val jsonTimePatition = CreateJsonString("TimePartitionInfo", configObj, Array(configObj.timePartition))
        json = json merge jsonTimePatition
      }
    }
    return json
  }
}
