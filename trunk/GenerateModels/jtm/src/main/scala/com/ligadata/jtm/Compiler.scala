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
package com.ligadata.jtm

import com.ligadata.jtm.eval.{Types => EvalTypes }
import com.ligadata.kamanja.metadata.{StructTypeDef, MdMgr}
import com.ligadata.kamanja.metadataload.MetadataLoad
import com.ligadata.messagedef.MessageDefImpl
import org.apache.commons.io.filefilter.TrueFileFilter
import org.apache.logging.log4j.{ Logger, LogManager }
import org.json4s.jackson.JsonMethods._
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

/** Class to collect all the parameter to build a compiler instance
  *
  */
class CompilerBuilder {

  def setSuppressTimestamps(switch: Boolean = true) = { suppressTimestamps = switch; this }
  def setInputFile(filename: String) = { inputFile = filename; this }
  def setOutputFile(filename: String) = { outputFile = filename; this }
  def setMetadataLocation(filename: String) = { metadataLocation = filename; this }

  var inputFile : String = null
  var outputFile : String = null
  var metadataLocation : String = null
  var suppressTimestamps : Boolean = false

  def build() : Compiler = {
    new Compiler(this)
  }
}

/* Translates a jtm (json) file(s) into scala classes
 *
 */
class Compiler(params: CompilerBuilder) extends LogTrait {

  /** Split a fully qualified object name into namspace and class
    *
    * @param name is a fully qualified class name
    * @return tuple with namespace and class name
    */
  def splitNamespaceClass(name: String): (String, String) = {
    val elements = name.split('.')
    (elements.dropRight(1).mkString("."), elements.last)
  }

  /** Creates a metadata instance with defaults and json objects located on the file system
    *
    * @return Metadata manager
    */
  def loadMetadata(): MdMgr= {

    val typesPath : String = ""
    val fcnPath : String = ""
    val attrPath : String = ""
    val msgCtnPath : String = ""
    val mgr : MdMgr = MdMgr.GetMdMgr

    val mdLoader = new MetadataLoad (mgr, typesPath, fcnPath, attrPath, msgCtnPath)
    mdLoader.initialize

    def getRecursiveListOfFiles(dir: File): Array[File] = {
      val these = dir.listFiles.filter(_.isFile)
      val those = dir.listFiles.filter(_.isDirectory)
      these ++ those.flatMap(getRecursiveListOfFiles)
    }

    val files = getRecursiveListOfFiles(new File(params.metadataLocation))

    // Load all json files for the metadata directory
    files.map ( jsonFile => {
      val json = FileUtils.readFileToString(jsonFile, null)
      val map = parse(json).values.asInstanceOf[Map[String, Any]]
      val msg = new MessageDefImpl()
      val ((classStrVer, classStrVerJava), msgDef, (classStrNoVer, classStrNoVerJava)) = msg.processMsgDef(json, "JSON", mgr, false)
      val msg1 = msgDef.asInstanceOf[com.ligadata.kamanja.metadata.MessageDef]
      mgr.AddMsg(msg1)
    })

    mgr
  }

  /** Find all logical column names that are encode in this expression $name
    *
    * @param expression
    * @return
    */
  def ExtractColumnNames(expression: String): Set[String] = {
    val regex = """(\$[a-zA-Z0-9_.]+)""".r
    regex.findAllMatchIn(expression).toArray.map( m => m.matched.drop(1)).toSet
  }

  /** Replace all logical column namess with the variables
    *
    * @param expression expression to update
    * @param mapNameSource name to variable mapping
    * @return string with the result
    */
  def FixupColumnNames(expression: String, mapNameSource: Map[String, String]): String = {
    val regex = """(\$[a-zA-Z0-9_.]+)""".r
    val m = regex.pattern.matcher(expression)
    val sb = new StringBuffer
    var i = 0
    while (m.find) {
      m.appendReplacement(sb, mapNameSource.get(m.group(0).drop(1)).get)
      i = i + 1
    }
    m.appendTail(sb)
    sb.toString
  }

  def Validate(root: Root) = {

    // Check requested language
    //
    if(root.language.trim.toLowerCase() !="scala")
        throw new Exception("Currenly only Scala is supported")

    // Check the min version
    //
    if(root.language.trim.toLowerCase=="scala") {
      // ToDo: Add version parser here
      if(root.minVersion.toDouble < 2.11) {
        throw new Exception("The minimum language requirement must be 2.11")
      }
    }

    if(root.imports.toSet.size < root.imports.size) {
      val dups = root.imports.groupBy(identity).collect { case (x,ys) if ys.size > 1 => x }
      logger.warn("Dropped duplicate imports: {}", dups.mkString(", "))
    }
  }

  def ColumnNames(mgr: MdMgr, classname: String): Set[String] = {
    val classinstance = md.Message(classname, 0, true)
    val members = classinstance.get.containerType.asInstanceOf[StructTypeDef].memberDefs
    members.map( e => e.Name).toSet
  }

  /**
    *
    * @param argName
    * @param className
    * @param fieldName
    */
  case class Element(argName: String, className: String, fieldName: String)

  def ColumnNames(mgr: MdMgr, classList: Set[String]): Array[Element] = {
    classList.foldLeft(1, Array.empty[Element])( (r, classname) => {
      val classMd = md.Message(classname, 0, true)
      val members = classMd.get.containerType.asInstanceOf[StructTypeDef].memberDefs
      (r._1 + 1, r._2 ++ members.map( e => Element("msg%d".format(r._1), classname, e.Name)))
    })._2
  }

  // Load metadata
  val md = loadMetadata

  val suppressTimestamps: Boolean = params.suppressTimestamps // Suppress timestamps
  val inputFile: String = params.inputFile // Input file to compile
  val outputFile: String = params.outputFile // Output file to write

  // Controls the code generation
  def Execute(): String = {

    // Load Json
    val root = Root.fromJson(inputFile)

    // Validate model
    Validate(root)

    var result = Array.empty[String]

    // Process header
    // ToDo: do we need a different license here
    result :+= Parts.header

    // Namespace
    //
    result :+= "package %s\n".format(root.namespace)

    // Process the imports
    //
    var subtitutions = new Substitution
    subtitutions.Add("model.name", root.namespace)
    subtitutions.Add("model.version", root.version)
    result :+= subtitutions.Run(Parts.imports)

    // Process additional imports
    //
    result ++= root.imports.distinct.map( i => "import %s".format(i) )

    // Collect all classes
    //
    val messages = EvalTypes.CollectMessages(root)
    messages.map( e => "%s aliases %s".format(e._1, e._2.mkString(", ")) ).foreach( m => {
      logger.trace(m)
    })

    // Collect all specified types
      // Should check we can resolve them
    val types = EvalTypes.CollectTypes(root)
    types.map( e => "%s usedby %s".format(e._1, e._2.mkString(", ")) ).foreach( m => {
      logger.trace(m)
    })


    // Check all found types against metadata
    //

    // Resolve dependencies
    //
    type aliasSet = Set[String]
    type transSet = Set[String]
    val dependencyToTransformations = root.transformations.foldLeft( (0, Map.empty[Set[String], (Long, Set[String])]))( (r1, t) => {
      val transformationName = t._1
      val transformation = t._2

      // Normalize the dependencies, target must be a class
      // ToDo: Do we need chains of aliases, or detect chains of aliases

      t._2.dependsOn.foldLeft(r1)( (r, dependencies) => {

        val resolvedDependencies = dependencies.map(alias => {
          // Translate dependencies, if available
          root.aliases.getOrElse( alias, alias )
        }).toSet

        val curr = r._2.get(resolvedDependencies)
        if(curr.isDefined) {
          ( r._1,     r._2 ++ Map[Set[String],(Long, Set[String])](resolvedDependencies -> (curr.get._1, curr.get._2 + t._1)) )
        } else {
          ( r._1 + 1, r._2 ++ Map[Set[String],(Long, Set[String])](resolvedDependencies -> (r._1 + 1, Set(t._1))) )
        }
      })

    })._2

    dependencyToTransformations.map( e => {
      "Dependency [%s] => (%s)".format(e._1.mkString(", "), e._2._2.mkString(", "))
    }).foreach( m =>logger.trace(m) )

    val errors = dependencyToTransformations.map( e => {

      if (e._2._2.size == 1) {

        // Emit function calls
        //
        val name = e._1.head
        val depId = e._2._1
        val calls = e._2._2.map( f => "exeGenerated_%s_%d(msg1)".format(f, depId) ).mkString("\n")
        result :+= """|if(msg.isInstanceOf[%s]) {
                      |val msg1 = msg.isInstanceOf[%s]
                      |%s
                      |}
                      |""".stripMargin('|').format(name, name, calls)
        0
      } else {
        logger.error("Unsupported multiple dependencies. {}", "Dependency [%s] => (%s)".format(e._1.mkString(", "), e._2._2.mkString(", ")))
        1
      }
    }).sum

    if(errors>0) {
      throw new Exception("Unsupported multiple dependencies found")
    }

    /*
    //
    //
    def ConstructIsValidMessage(inputTypes: Array[String]): String = {
      inputTypes.map( e => "    msg.isInstanceOf[%s]".format(e) ).mkString("||\n") + "\n"
    }
    subtitutions.Add("factory.isvalidmessage", ConstructIsValidMessage(inputMap.map( p => p._2.handle )))
    val factory = subtitutions.Run(Parts.factory)
    */

    // Actual function to be called
    //
    dependencyToTransformations.foreach( e => {
      val deps = e._1
      val depId = e._2._1
      val transformationNames = e._2._2

      transformationNames.foreach( t => {

        val transformation = root.transformations.get(t).get

        result :+= "def exeGenerated_%s_%d(msg: %s) = {".format(t, depId, deps.head)

        // Collect form metadata
        val inputs: Array[Element] = ColumnNames(md, deps) // Seq("in1", "in2", "in3", "in4").toSet

        // Resolve inputs, either we have unique or qualified names
        //
        val uniqueInputs = {
          val u = inputs.map( e => e.fieldName ).groupBy(identity).mapValues(_.size).filter( f => f._2==1).map( p => p._1)
          val u1 = u.map( e => inputs.find( c => c.fieldName == e).get)
          u1.map( p => (p.fieldName -> "%s.%s".format(p.argName, p.fieldName)))
        }.toMap

        val qualifiedInputs = inputs.map( p => {
          (p.className + "." + p.fieldName -> "%s.%s".format(p.argName, e))
        }).toMap

        var mappingSources = uniqueInputs ++ qualifiedInputs

        // Common computes section
        //
        var computes = transformation.computes
        var cnt1 = computes.size
        var cnt2 = 0

        while(cnt1!=cnt2 && computes.size > 0) {
          cnt2 = cnt1

          val computes1 = computes.filter(c => {

            val list = ExtractColumnNames(c._2.expression)
            val rList = list // ResolveNames(list)
            val open = rList.filter(f => !mappingSources.contains(f) )
            if(open.size==0) {
              val newExpression = FixupColumnNames(c._2.expression, mappingSources)

              // Output the actual compute
              result ++= Array("val %s = %s\n".format(c._1, newExpression))
              mappingSources ++= Map(c._1 -> c._1)
              false
            } else {
              true
            }
          })

          cnt1 = computes1.size
          computes = computes1
        }

        if(cnt2!=0){
          //throw new Exception("Not all elements used")
          logger.trace("Not all elements used")
        }

/*
        // Individual outputs
        //
        tr ++= transformation.outputs.foldLeft(Array.empty[String]) ( (r, o) => {

          var collect = Array.empty[String]
          collect ++= Array("\ndef fn%d() = {\n".format(fncnt))

          val outputSet: Set[String] = ColumnNames(md, o.output) //Seq("out1", "out2", "out3", "out4").toSet

          // State variables to track the progress
          // a little bit simpler than having val's
          var mapNameSource: Map[String, String] = inputSet.map( e => (e,"msg.%s".format(e))).toMap
          var outputSet1: Set[String] = outputSet

          var mapping = o.mappings
          var filters =  o.filters
          var computes = o.computes
          var cnt1 = filters.length + computes.length
          var cnt2 = 0

          // Removed if mappings are provided
          val found = mapping.filter( f => mapNameSource.contains(f._2) )
          found.foreach( f => { outputSet1 --= Set(f._1); mapNameSource ++= Map(f._1 -> mapNameSource.get(f._2).get) } )
          mapping = mapping.filterKeys( f => !found.contains(f) )

          // Abort this loop if nothing changes or we can satisfy all outputs
          while(cnt1!=cnt2 && outputSet1.size > 0) {

            cnt2 = cnt1

            // filters
            val filters1 = filters.filter(f => {
              val list = ExtractColumnNames(f.expression)
              val open = list.filter(f => !mapNameSource.contains(f) )
              if(open.size==0) {
                // Sub names to
                val newExpression = FixupColumnNames(f.expression, mapNameSource)
                // Output the actual filter
                collect ++= Array("if (%s) return\n".format(newExpression))
                false
              } else {
                true
              }
            })

            // computes
            val computes1 = computes.filter( c => {
              val list = ExtractColumnNames(c.expression)
              val open = list.filter(f => !mapNameSource.contains(f) )
              if(open.size==0) {
                // Sub names to
                val newExpression = FixupColumnNames(c.expression, mapNameSource)
                // Output the actual compute
                collect ++= Array("val %s = %s\n".format(c.output, newExpression))
                mapNameSource ++= Map(c.output -> c.output)
                outputSet1 --= Set(c.output)
                false
              } else {
                true
              }
            })

            // Check Mapping
            if(mapping.size>0)
            {
              val found = mapping.filter( f => mapNameSource.contains(f._2) )
              found.foreach(f => {outputSet1 --= Set(f._1); mapNameSource ++= Map(f._1 -> mapNameSource.get(f._2).get)})
              mapping = mapping.filterKeys( f => !found.contains(f)  )
            }

            // Update state
            cnt1 = filters1.length + computes1.length
            filters = filters1
            computes = computes1
          }

          // Transaction id is in the input
          // so will just push it back if needed
          if(inputSet.contains("transactionid")) {
            outputSet1 --= Set("transactionid")
          }

          if(outputSet1.size>0){
            throw new Exception("Not all outputs satisfied. missing=" + outputSet1.mkString(", "))
            logger.trace("Not all outputs satisfied. missing={}" , outputSet1.mkString(", "))
          }

          if(cnt2!=0){
            //throw new Exception("Not all elements used")
            logger.trace("Not all elements used")
          }

          // Generate the output for this iteration
          // Translate outputs to the values
          val outputElements = outputSet.map( e => {
            // e.name -> from input, from mapping, from variable
            "new Result(\"%s\", %s)".format(e, mapNameSource.get(e).get)
          }).mkString(", ")
          val outputResult = "result ++= Array[Result](%s)".format(outputElements)

          collect ++= Array(outputResult)
          collect ++= Array("}\n")
          collect ++= Array("fn%d()\n".format(fncnt))

          // outputs
          r ++ collect
        })
        result :+= "}"
*/
      })

    })

    // Create metadata and factory code
    //

    // Rename types to short names
    //

    // Process inputs
    //

    // Write to output file
    val code = CodeHelper.Indent(result)
    logger.trace("Output to file {}", outputFile)
    FileUtils.writeStringToFile(new File(outputFile), code)

    outputFile
  }

  /*
  def CollectInputs(t: Array[Transformation]): Array[String] = {
    val s = t.map( e => {
      e.input
    })
    s.toSet.toArray
  }

  def CollectOutputs(t: Array[Transformation]): Array[String] = {
    val s = t.foldLeft(Array.empty[String]) ( (r, e) => {
      val s = e.outputs.foldLeft(r) ( (r, e) => {
        r ++ Array(e.output)
      })
      r ++ s
    })
    s.toSet.toArray
  }

  def FixupColumnNames(expression: String, mapNameSource: Map[String, String]): String = {
    val regex = """(\$[a-zA-Z0-9_]+)""".r
    val m = regex.pattern.matcher(expression)
    val sb = new StringBuffer
    var i = 0;
    while (m.find) {
      m.appendReplacement(sb, mapNameSource.get(m.group(0).drop(1)).get)
      i = i + 1
    }
    m.appendTail(sb)
    sb.toString
  }

  def ColumnNames(mgr: MdMgr, classname: String): Set[String] = {
    val classinstance = md.Message(classname, 0, true)
    val members = classinstance.get.containerType.asInstanceOf[StructTypeDef].memberDefs
    members.map( e => e.Name).toSet
  }

  def Execute(): String = {

    var fncnt = 0

    case class leg(val packagename: String, val classname: String, val handle: String, val id: Int)

    // Constructs the input and output types
    val inputMap = CollectInputs(root.transformations).zipWithIndex.map( e => {
      val (packagename, classname) = splitPackageClass(e._1)
      val handle = "mi%d".format(e._2)
      ( e._1 -> leg(packagename, classname, handle, e._2))
    })

    val outputMap = CollectOutputs(root.transformations).zipWithIndex.map( e => {
      val (packagename, classname) = splitPackageClass(e._1)
      val handle = "mo%d".format(e._2)
      ( e._1 -> leg(packagename, classname, handle, e._2))
    })

    val inputs = inputMap.map( p => {
        val leg = p._2
        "import %s.{%s ⇒ %s}".format(leg.packagename, leg.classname, leg.handle)
    }).mkString("\n")
    sb.append(inputs)
    sb.append("\n")

    val outputs = outputMap.map( p => {
      val leg = p._2
      "import %s.{%s ⇒ %s}".format(leg.packagename, leg.classname, leg.handle)
    }).mkString("\n")
    sb.append(outputs)
    sb.append("\n")

    subtitutions.Add("factory.isvalidmessage", ConstructIsValidMessage(inputMap.map( p => p._2.handle )))
    val factory = subtitutions.Run(Parts.factory)
    sb.append(factory)
    sb.append("\n\n")

    // Collect all outputs here
    val resultVar = "var result: Array[Result] = Array.empty[Result]"

    val inputprocessing = inputMap.map( p => {
      val leg = p._2

      // Get all transformations attached to the input
      val transformations = root.transformations.filter( t => t.input == p._1 )

      val transparts = transformations.foldLeft(Array.empty[Array[String]]) ( (r, t) => {

        var tr = Array.empty[String]

        // Process the output per message

        // variables
        tr ++= t.variables.map( e => "val %s: %s".format(e.name, e.typename))

        tr ++= t.outputs.foldLeft(Array.empty[String]) ( (r, o) => {

          var collect = Array.empty[String]
          collect ++= Array("\ndef fn%d() = {\n".format(fncnt))

          // Collect form metadata
          val inputSet: Set[String] = ColumnNames(md, t.input) // Seq("in1", "in2", "in3", "in4").toSet
          val outputSet: Set[String] = ColumnNames(md, o.output) //Seq("out1", "out2", "out3", "out4").toSet

          // State variables to track the progress
          // a little bit simpler than having val's
          var mapNameSource: Map[String, String] = inputSet.map( e => (e,"msg.%s".format(e))).toMap
          var outputSet1: Set[String] = outputSet

          var mapping = o.mappings
          var filters =  o.filters
          var computes = o.computes
          var cnt1 = filters.length + computes.length
          var cnt2 = 0

          // Removed if mappings are provided
          val found = mapping.filter( f => mapNameSource.contains(f._2) )
          found.foreach( f => { outputSet1 --= Set(f._1); mapNameSource ++= Map(f._1 -> mapNameSource.get(f._2).get) } )
          mapping = mapping.filterKeys( f => !found.contains(f) )

          // Abort this loop if nothing changes or we can satisfy all outputs
          while(cnt1!=cnt2 && outputSet1.size > 0) {

            cnt2 = cnt1

            // filters
            val filters1 = filters.filter(f => {
              val list = ExtractColumnNames(f.expression)
              val open = list.filter(f => !mapNameSource.contains(f) )
              if(open.size==0) {
                // Sub names to
                val newExpression = FixupColumnNames(f.expression, mapNameSource)
                // Output the actual filter
                collect ++= Array("if (%s) return\n".format(newExpression))
                false
              } else {
                true
              }
            })

            // computes
            val computes1 = computes.filter( c => {
              val list = ExtractColumnNames(c.expression)
              val open = list.filter(f => !mapNameSource.contains(f) )
              if(open.size==0) {
                // Sub names to
                val newExpression = FixupColumnNames(c.expression, mapNameSource)
                // Output the actual compute
                collect ++= Array("val %s = %s\n".format(c.output, newExpression))
                mapNameSource ++= Map(c.output -> c.output)
                outputSet1 --= Set(c.output)
                false
              } else {
                true
              }
            })

            // Check Mapping
            if(mapping.size>0)
            {
              val found = mapping.filter( f => mapNameSource.contains(f._2) )
              found.foreach(f => {outputSet1 --= Set(f._1); mapNameSource ++= Map(f._1 -> mapNameSource.get(f._2).get)})
              mapping = mapping.filterKeys( f => !found.contains(f)  )
            }

            // Update state
            cnt1 = filters1.length + computes1.length
            filters = filters1
            computes = computes1
          }

          // Transaction id is in the input
          // so will just push it back if needed
          if(inputSet.contains("transactionid")) {
            outputSet1 --= Set("transactionid")
          }

          if(outputSet1.size>0){
            throw new Exception("Not all outputs satisfied. missing=" + outputSet1.mkString(", "))
            logger.trace("Not all outputs satisfied. missing={}" , outputSet1.mkString(", "))
          }

          if(cnt2!=0){
            //throw new Exception("Not all elements used")
            logger.trace("Not all elements used")
          }

          // Generate the output for this iteration
          // Translate outputs to the values
          val outputElements = outputSet.map( e => {
            // e.name -> from input, from mapping, from variable
            "new Result(\"%s\", %s)".format(e, mapNameSource.get(e).get)
          }).mkString(", ")
          val outputResult = "result ++= Array[Result](%s)".format(outputElements)

          collect ++= Array(outputResult)
          collect ++= Array("}\n")
          collect ++= Array("fn%d()\n".format(fncnt))

          // outputs
          r ++ collect
        })

        r ++ Array(tr)
      })

      "if(msg.isInstanceOf[%s]) {\n".format(leg.handle) +
      "val msg = msg.isInstanceOf[%s]\n".format(leg.handle) +
      transparts.map( e => e.mkString("\n") + "\n").mkString("\n") +
      "\n}"
    })

    val returnValue = "factory.createResultObject().asInstanceOf[MappedModelResults].withResults(result)"

    subtitutions.Add("model.methods", "")
    subtitutions.Add("model.code", resultVar + "\n\n" + inputprocessing.mkString("\n") + "\n\n" + returnValue + "\n")
    val model = subtitutions.Run(Parts.model)
    sb.append(model)
    sb.append("\n")
  }
  */
}
