package com.ligadata.jtm.nodes

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonSerializer
import com.google.gson.JsonDeserializer
import com.google.gson.JsonElement
import java.lang.reflect.Type
import com.google.gson._
import scala.collection.JavaConversions._
import com.google.gson.reflect.TypeToken
import com.google.gson.stream.JsonReader
import org.apache.commons.io.FileUtils
import java.io.StringReader
import java.io.File

object Root {

  type MapType = scala.collection.Map[String, String]

  class MapType1 extends JsonDeserializer[MapType] with JsonSerializer[MapType] {

    def deserialize(json : JsonElement, typeOfT : Type , context : JsonDeserializationContext) : scala.collection.Map[String, String] = {

      var collectMap = scala.collection.mutable.HashMap.empty[String, String]
      val entrySet = json.getAsJsonObject().entrySet()
      entrySet.map ( entry =>  collectMap += ( entry.getKey() -> entry.getValue.getAsString() ) )

      collectMap
    }

    def serialize(src : scala.collection.Map[String, String], typeOfT : Type , context : JsonSerializationContext) :  JsonObject = {
      val json = new JsonObject
      src.foreach( p => json.addProperty(p._1, p._2)
      )
      json
    }
  }

  def fromJsonString(config : String) : Root = {
    val typeMap = new TypeToken[scala.collection.Map[String, String]](){}.getType()
    val gson = new GsonBuilder().registerTypeAdapter(typeMap, new MapType1).create()
    val reader = new JsonReader(new StringReader(config))
    reader.setLenient(true)
    gson.fromJson(reader, classOf[Root])
  }

  def fromJson(file : String) : Root = {
    val config = FileUtils.readFileToString(new File(file), null)
    fromJsonString(config)
  }

  def toJson(file: String, c: Root) = {
    val typeMap = new TypeToken[scala.collection.Map[String, String]](){}.getType()
    val gson = new GsonBuilder().registerTypeAdapter(typeMap, new MapType1).setPrettyPrinting().create()
    val dataJson = gson.toJson(c)
    scala.tools.nsc.io.File(file).writeAll(dataJson)
  }

  def toJson(c: Root) : String = {
    val typeMap = new TypeToken[scala.collection.Map[String, String]](){}.getType()
    val gson = new GsonBuilder().registerTypeAdapter(typeMap, new MapType1).setPrettyPrinting().create()
    gson.toJson(c)
  }
}

/**
  * Created by joerg on 1/20/16.
  */
class Root {
  val packagename: String = ""
  val version: String = ""
  val modelname : String = ""

  val imports: Array[String] = Array.empty[String]
  val transformations: Array[Transformation] = Array.empty[Transformation]
}
