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

    def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): MapType = {
      var collectMap = scala.collection.Map.empty[String, String]
      val entrySet = json.getAsJsonObject().entrySet()
      entrySet.map( entry =>  collectMap += ( entry.getKey() -> entry.getValue.getAsString()) )
      collectMap
    }

    def serialize(src: MapType, typeOfT: Type, context: JsonSerializationContext) :  JsonObject = {
      val json = new JsonObject
      src.foreach( p => json.addProperty(p._1, p._2) )
      json
    }
  }

  import scala.reflect.ClassTag
  import scala.reflect._
  class MapToType[T:ClassTag] extends JsonDeserializer[scala.collection.Map[String, T]] with JsonSerializer[scala.collection.Map[String, T]] {

    def deserialize(json: JsonElement, typeOfT1: Type, context: JsonDeserializationContext): scala.collection.Map[String, T] = {
      var map = scala.collection.Map.empty[String, T]
      val p = json.getAsJsonObject()
      p.entrySet().map( e => {
        val o = e.getValue
        val t = classTag[T].runtimeClass
        val v: T = context.deserialize[T](o, t)
        map ++= Map( e.getKey -> v)
      })
      map
    }

    def serialize(src: scala.collection.Map[String, T], typeOfT: Type, context: JsonSerializationContext):  JsonObject = {
      val json = new JsonObject
      src.foreach( p => {
        val jo = context.serialize(p._2).getAsJsonObject()
        json.add(p._1, jo)
      })
      json
    }
  }

  // Setup  the gson object to be used
  def buildGson(): Gson = {
    val mapToString = new TypeToken[scala.collection.Map[String, String]](){}.getType()
    val mapToTransformation = new TypeToken[scala.collection.Map[String, Transformation]](){}.getType()
    val mapToOutput = new TypeToken[scala.collection.Map[String, Output]](){}.getType()
    val mapToCompute = new TypeToken[scala.collection.Map[String, Compute]](){}.getType()

    new GsonBuilder().
      registerTypeAdapter(mapToString, new MapType1).
      registerTypeAdapter(mapToTransformation, new MapToType[Transformation]).
      registerTypeAdapter(mapToOutput, new MapToType[Output]).
      registerTypeAdapter(mapToCompute, new MapToType[Compute]).
      create()
  }

  val gson = buildGson()

  def fromJsonString(config : String) : Root = {
    val reader = new JsonReader(new StringReader(config))
    reader.setLenient(true)
    gson.fromJson(reader, classOf[Root])
  }

  def fromJson(file : String) : Root = {
    val config = FileUtils.readFileToString(new File(file), null)
    fromJsonString(config)
  }

  def toJson(file: String, c: Root) = {
    val dataJson = gson.toJson(c)
    scala.tools.nsc.io.File(file).writeAll(dataJson)
  }

  def toJson(c: Root) : String = {
    gson.toJson(c)
  }
}

/**
  * Created by joerg on 1/20/16.
  */
class Root {
  val namespace: String = ""
  val version: String = ""
  val language: String = ""
  val minVersion: String = ""
  val dependencies: Array[String] = Array.empty[String]
  val imports: Array[String] = Array.empty[String]

  val transformations: scala.collection.Map[String, Transformation] = scala.collection.Map.empty[String, Transformation]
  val aliases: scala.collection.Map[String, String] = scala.collection.Map.empty[String, String]
}
