package com.ligadata.jtm.nodes

/**
  * Created by joerg on 1/20/16.
  */
class Output {
  val mapping: scala.collection.Map[String, String] = scala.collection.Map.empty[String, String]
  val filter: String = ""
  val computes: scala.collection.Map[String, Compute] = scala.collection.Map.empty[String, Compute]
  val mappingByName: Boolean = false
}
