/*
 * Copyright 2015 ligaDATA
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
package com.ligadata.jtm

import org.apache.logging.log4j.{ Logger, LogManager }
import org.rogach.scallop._
import org.apache.commons.io.FileUtils
import java.io.File

import com.ligadata.jtm.nodes._

object jtmGlobalLogger {
  val loggerName = this.getClass.getName()
  val logger = LogManager.getLogger(loggerName)
}

trait LogTrait {
  val logger = jtmGlobalLogger.logger
}

class Conf (arguments: Seq[String] ) extends ScallopConf (arguments)  with LogTrait {

  val jtm = opt[String] (required = true, descr = "Sources to compile", default = None )
/*
  val scalahome = opt[String] (required = false, descr = "", default = Some ("") )
  val javahome = opt[String] (required = false, descr = "", default = Some ("") )
  val cp = opt[String] (required = false, descr = "", default = Some ("") )
  val jarpath = opt[String] (required = false, descr = "", default = Some ("") )
  val scriptout = opt[String] (required = false, descr = "Sources to compile", default = Some ("") )
  val manifest = opt[String] (required = false, descr = "Sources to compile", default = Some ("") )
  val client = opt[String] (required = false, descr = "Sources to compile", default = Some ("") )
  val sourceout = opt[String] (required = false, descr = "Path to the location to store generated sources", default = Some ("") )
  val addLogging = opt[Boolean] (required = false, descr = "Add logging code to the model", default = Some (true) )
  val createjar = opt[Boolean] (required = false, descr = "Create the final jar output ", default = Some (true) )
*/
}

/* Commandline interface to compiler
 *
 */
object Compiler extends App with LogTrait {
  override def main (args: Array[String] ) {

      try {
        val cmdconf = new Conf(args)
      }
      catch {
        case e: Exception => {
          System.exit(1)
        }
      }
      // Do all validations

      // Create compiler instance and generate scala code
  }
}

object CompilerBuilder {
  def create() = { new CompilerBuilder }
}

class CompilerBuilder {

  def setSuppressTimestamps(switch: Boolean = true) = { suppressTimestamps = switch; this}
  def setInputFile(filename: String) = { inputFile = filename; this }
  def setOutputFile(filename: String) = { outputFile = filename; this }

  var inputFile : String = null
  var outputFile : String = null
  var suppressTimestamps : Boolean = false

  def build() : Compiler = {
    new Compiler(this)
  }
}



/* Translates a jtm (json) file(s) into scala classes
 *
 */
class Compiler(params: CompilerBuilder) extends LogTrait {

  def splitPackageClass(name: String): (String, String) = {
    val elements = name.split('.')
    (elements.dropRight(1).mkString("."), elements.last)
  }

  val suppressTimestamps: Boolean = params.suppressTimestamps // Suppress timestamps
  val inputFile: String = params.inputFile // Input file to compile
  val outputFile: String = params.outputFile // Output file to write

  def Execute(): String = {

    // Load Json
    val root = Root.fromJson(inputFile)

    val sb = new StringBuilder
    sb.append(Parts.header)

    // Push substituions
    var subtitutions = new Substitution
    subtitutions.Add("model.name", "filter")
    subtitutions.Add("model.version", root.version)

    val imports = subtitutions.Run(Parts.imports)
    sb.append(imports)
    sb.append("\n")
    
    val factory = subtitutions.Run(Parts.factory)
    sb.append(factory)
    sb.append("\n")

    // Constructs the input and output types
    val inputs = root.inputs.zipWithIndex.map( p => {
      val (packagename, classname) = splitPackageClass(p._1.typename)
        "import %s.{%s ⇒ input%d}".format(packagename, classname, p._2)
    }).mkString("\n")
    sb.append(inputs)
    sb.append("\n")

    val outputs = root.outputs.zipWithIndex.map( p => {
      val (packagename, classname) = splitPackageClass(p._1.typename)
      "import %s.{%s ⇒ output%d}".format(packagename, classname, p._2)
    }).mkString("\n")
    sb.append(outputs)
    sb.append("\n")

    // Read the output type information

    // Construct input

    // Construct filter

    // Construct output

    // Write to output file
    FileUtils.writeStringToFile(new File(outputFile), sb.result)

    outputFile
  }
}
