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

class JsonUtility {
  def CreateMainJsonString(data: Map[String, String], configObj: ConfigBean):JsonAST.JValue={
    val json =
      ("Meesage" ->
        ("NameSpace" -> configObj.nameSpace) ~
          ("Name" -> configObj.messageName)~
          ("Verion" -> "00.01.00")~
          ("Description" -> "")~
          ("Fixed" -> configObj.messageType.toString)~
          ("Persist" -> configObj.saveMessage)~
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

  def CreateJsonString (feild: String,  configObj: ConfigBean):JsonAST.JValue={
    var json:JsonAST.JValue = ""
    if(!feild.equalsIgnoreCase("TimePartitionInfo")) {
       json = ("Meesage" ->
        ("NameSpace" -> configObj.nameSpace) ~
          ("Name" -> configObj.messageName) ~
          ("Verion" -> "00.01.00") ~
          ("Description" -> "") ~
          ("Fixed" -> configObj.messageType.toString) ~
          ("Persist" -> configObj.saveMessage) ~
          (feild -> List.empty[JInt])
        )
    } else {
       json = ("Meesage" ->
        ("NameSpace" -> configObj.nameSpace) ~
          ("Name" -> configObj.messageName) ~
          ("Verion" -> "00.01.00") ~
          ("Description" -> "") ~
          ("Fixed" -> configObj.messageType.toString) ~
          ("Persist" -> configObj.saveMessage) ~
          (feild -> ("Key" -> "") ~
            ("Format" -> "epochtime") ~
            ("Type" -> "Daily"))
        )
    }
    return json
  }
}
