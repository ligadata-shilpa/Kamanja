package com.ligadata.jtm.nodes

/**
  * Created by joerg on 1/26/16.
  */
class OutputMessage {
  val output: String = ""
  val mappings: scala.collection.Map[String, String] = Map.empty[String, String]
  val filters: Array[Filter] = Array.empty[Filter]
  val computes: Array[Compute] = Array.empty[Compute]
}
