package com.ligadata.jtm.nodes

/**
  * Created by joerg on 1/20/16.
  */
class Transformation {
  val name: String = ""
  val inputs: Array[String] = Array.empty[String]
  val computes: scala.collection.Map[String, Compute] = scala.collection.Map.empty[String, Compute]
  val outputs: scala.collection.Map[String, Output] = scala.collection.Map.empty[String, Output]
  val comment: String = ""
  val comments: Array[String] = Array.empty[String]
  val dependsOn: Array[Array[String]] = Array.empty[Array[String]]
}
