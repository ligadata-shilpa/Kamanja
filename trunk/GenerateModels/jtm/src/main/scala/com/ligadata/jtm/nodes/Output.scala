package com.ligadata.jtm.nodes

/**
  * Created by joerg on 1/20/16.
  */
class Output {
  val mappings: scala.collection.Map[String, String] = scala.collection.Map.empty[String, String]
  val filter: String = ""
  val computes: Computes = new Computes
  val mappingByName: Boolean = false
}
