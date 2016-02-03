/*
 * Copyright 2016 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
