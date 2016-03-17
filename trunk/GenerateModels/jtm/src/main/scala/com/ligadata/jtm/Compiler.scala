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

import com.ligadata.jtm.eval.{Types => EvalTypes, Stamp, Expressions, GrokHelper}
import com.ligadata.kamanja.metadata._
import com.ligadata.kamanja.metadataload.MetadataLoad
import com.ligadata.messagedef.MessageDefImpl
import org.aicer.grok.dictionary.GrokDictionary
import org.apache.logging.log4j.{ Logger, LogManager }
import org.json4s.jackson.JsonMethods._
import org.rogach.scallop._
import org.apache.commons.io.FileUtils
import java.io.{StringReader, File}

import com.ligadata.jtm.nodes._

// Laundry list
/*
3) Support java
4) Plug into kamanja metadata tool

a) End-End-Test
b) Generate code validation for unit test => compile
c) Model run and validation for unit test => compile, run
*/

object jtmGlobalLogger {
  val loggerName = this.getClass.getName
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
        case e: Exception => System.exit(1)
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
  def setMetadata(md: MdMgr) = { metadataMgr = md; this }
  def setJtm(jtm: String) = { jtmData = jtm; this }

  var jtmData: String = null
  var inputFile: String = null
  var outputFile: String = null
  var metadataLocation: String = null
  var suppressTimestamps: Boolean = false
  var metadataMgr: MdMgr = null

  def build() : Compiler = {
    new Compiler(this)
  }
}

/* Translates a jtm (json) file(s) into scala classes
 *
 */
class Compiler(params: CompilerBuilder) extends LogTrait {

  /** Initialize from parameter block
    *
    */
  val md = if(params.metadataMgr==null) {
    loadMetadata(params.metadataLocation) // Load metadata if not passed in
  } else {
    params.metadataMgr
  }

  val suppressTimestamps: Boolean = params.suppressTimestamps // Suppress timestamps

  val inputFile: String = params.inputFile // Input file to compile
  val outputFile: String = params.outputFile // Output file to write
  val root = Root.fromJson(inputFile) // Load Json

  private var code: String = null // The generated code

  /**
    * Collect information needed for modeldef
    */
  private var inmessages = Array.empty[Map[String, Set[String]]] // Records all sets of incoming classes and attributes accessed
  private var outmessages = Set.empty[String] //Records all outgoing classes

  /** Returns the modeldef after compiler completed
    *
    */
  def MakeModelDef() : ModelDef = {

    if(code==null)
      throw new Exception("No code was successful created")

    val supportsInstanceSerialization : Boolean = false
    val isReusable: Boolean = true

    val out: Array[String] = outmessages.toArray

    val in: Array[Array[MessageAndAttributes]] = inmessages.map( s =>
          s.map( m => {
            val ma = new MessageAndAttributes
            ma.message = m._1
            ma.attributes = m._2.toArray
            ma
          }).toArray
    )

    /*
    val in = inmessages.map( m =>  {
      val msg = new MessageAndAttributes
      msg.message = m._1
      msg.attributes = m._2.toArray
      msg
    }).toArray
    */
    /*
     val modelRepresentation: ModelRepresentation = ModelRepresentation.JAR
     val miningModelType : MiningModelType = MiningModelType.UNKNOWN
     val inputVars : Array[BaseAttributeDef] = null
     val outputVars: Array[BaseAttributeDef] = null
     val isReusable: Boolean = false
     val msgConsumed: String = ""
     val supportsInstanceSerialization : Boolean = false
     */
    new ModelDef(ModelRepresentation.JAR, MiningModelType.UNKNOWN, in, out, isReusable, supportsInstanceSerialization)
  }

  def Code() : String = {
    if(code==null)
      throw new Exception("No code was successful created")

    code
  }

  /** Split a fully qualified object name into namspace and class
    *
    * @param name is a fully qualified class name
    * @return tuple with namespace and class name
    */
  def splitNamespaceClass(name: String): (String, String) = {
    val elements = name.split('.')
    (elements.dropRight(1).mkString("."), elements.last)
  }

  def Validate(root: Root) = {

    // Check requested language
    //
    if(root.header==null)
      throw new Exception("No header provided")

    if(root.aliases!=null) {
      if(root.aliases.concepts.size>0) {
        throw new Exception("Currently concepts aren't supported")
      }
      if(root.aliases.variables.size>0) {
        throw new Exception("Currently variables aren't supported")
      }
    }

    val header = root.header

    if(header.language.trim.toLowerCase() !="scala")
        throw new Exception("Currently only Scala is supported")

    // Check the min version
    //
    if(header.language.trim.toLowerCase=="scala") {
      // ToDo: Add version parser here
      if(header.minVersion.toDouble < 2.10) {
        throw new Exception("The minimum language requirement must be 2.10")
      }
    }

    if(root.imports.packages.toSet.size < root.imports.packages.length) {
      val dups = root.imports.packages.groupBy(identity).collect { case (x,ys) if ys.length > 1 => x }
      logger.warn("Dropped duplicate imports: {}", dups.mkString(", "))
    }

    // Validate any computes nodes if val and vals are set
    val computeConstraint = root.transformations.foldLeft("root/", Map.empty[String, String])( (r, t) => {

      val r1 = t._2.computes.foldLeft(r._1 + t._1 + "/", Map.empty[String, String])( (r, c) => {
        if(c._2.expression.length > 0 && c._2.expressions.length > 0) {
          ("", r._2 ++ Map(r._1 + c._1 -> "Vals and val attribute are set, please choose one.") )
        } else {
          r
        }
      })._2

      val r2 = t._2.outputs.foldLeft(r._1 + t._1 + "/", Map.empty[String, String])( (r, o) => {
        o._2.computes.foldLeft( r._1 + o._1 + "/", r._2 )((r, c) => {
          if(c._2.expression.length > 0 && c._2.expressions.length > 0) {
            ("", r._2 ++ Map(r._1 + c._1 -> "Vals and val attribute are set, please choose one.") )
          } else {
            r
          }
        })
      })._2

      ("", r1 ++ r2)
    })._2

    if(computeConstraint.nonEmpty) {
      computeConstraint.foreach( m => logger.warn(m.toString()))
      throw new Exception("Conflicting %d compute nodes".format(computeConstraint.size))
    }

    // Check that we only have a single grok instance
    if(root.grok.size>1) {
      throw new Exception("Found %d grok configurations, only a single configuration allowed.".format(root.grok.size))
    }

    // Validate any grok configuration
    GrokHelper.Validate(root)
  }

  /** Escape string as literal
    *
    * @param raw
    * @return
    */
  def escape(raw: String): String = {
    import scala.reflect.runtime.universe._
    Literal(Constant(raw)).toString
  }
  /** Product the configuration
    *
    * @param grok
    * @return
    */
  def BuildGrokInstance(grok : Grok): Array[String] = {

    var result = Array.empty[String]

    result :+= "lazy val grok_instance_1: GrokDictionary = {"
    result :+= "val dict = new GrokDictionary"

    if(grok.builtInDictionary)
      result :+= "dict.addBuiltInDictionaries"

    result ++= grok.file.distinct.map(
      f => {
        val file = new File(f)
        val name = "\"grok/%08X/%s\"".format(f.hashCode, file.getName)
        s"dict.addDictionary(new File(getClass.getResource($name).getPath))"
    })

    result ++= grok.patterns.map( p => {
      "dict.addDictionary(new StringReader(%s)".format(escape(p._1 + " " + p._2))
    })

    result :+= "dict.bind()"
    result :+= "dict"
    result :+= "}"
    result
  }

  /** Emit code to compile the unique expressions
    *
    * @param root
    * @return
    */
  def BuildGrokCompiledExpression(root: Root) : (Array[String], Map[String, (String, String, Set[String])]) = {

    var result = Array.empty[String]

    // Get the root instance - we only support 1 right now
    val grok : Grok = root.grok.head._2
    //val grokInstance: GrokDictionary = GrokHelper.Validate(grok)

    // Collect all unique expressions
    val expressions = root.transformations.foldLeft(Set.empty[String])((r, t) => {
      t._2.grokMatch.foldLeft(r) ((r, m) => {
        r + m._2
      })
    }).zipWithIndex.toMap

    val mapping = expressions.map( p => {

      val index = p._2
      val expressionString = p._1
      val outputs = GrokHelper.ExtractDictionaryKeys(expressionString)
      val pattern =  GrokHelper.ConvertToGrokPattern(expressionString)

      expressionString -> ("grok_instance_1_%d".format(p._2), pattern, outputs)
    }).toMap

    val defs = mapping.map( p=>
      "lazy val %s = grok_instance_1.compileExpression(%s)".format(p._2._1, escape(p._2._2))
    ).toArray

    (defs, mapping)
  }
  // Casing of system columns is inconsistent
  // provide a patch up map
  val columnNamePatchUp = Map.empty[String, String]
  val columnSystem = Set("transactionId", "rowNumber", "timePartitionData")

  def ColumnNames(mgr: MdMgr, classname: String): Set[String] = {
    val classinstance = md.Message(classname, 0, true)
    if(classinstance.isEmpty) {
      throw new Exception("Metadata: unable to find class %s".format(classname))
    }
    val members = classinstance.get.containerType.asInstanceOf[StructTypeDef].memberDefs
    members.map( e => columnNamePatchUp.getOrElse(e.Name, e.Name)).toSet
  }

  def ResolveToVersionedClassname(mgr: MdMgr, classname: String): String = {
    val classinstance = md.Message(classname, 0, true)
    if(classinstance.isEmpty) {
      throw new Exception("Metadata: unable to find class %s".format(classname))
    }
    // We convert to lower case
    classinstance.get.physicalName.toLowerCase
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
      (r._1 + 1, r._2 ++ members.map( e => Element("msg%d".format( r._1), classname, columnNamePatchUp.getOrElse(e.Name, e.Name))))
    })._2
  }

  def ResolveNames(names: Set[String], aliaseMessages: Map[String, String] ) : Map[String, String] =  {
    Expressions.ResolveNames(names, aliaseMessages)
  }

  def ResolveName(n: String, aliaseMessages: Map[String, String] ) : String =  {
    Expressions.ResolveName(n, aliaseMessages)
  }

  def ResolveAlias(n: String, aliaseMessages: Map[String, String] ) : String =  {
    Expressions.ResolveAlias(n, aliaseMessages)
  }

  // Controls the code generation
  def Execute(): String = {

    // Validate model
    Validate(root)

    val aliaseMessages: Map[String, String] = root.aliases.messages.toMap
    var groks = Array.empty[String]
    var result = Array.empty[String]
    var exechandler = Array.empty[String]
    var methods = Array.empty[String]
    var messages = Array.empty[String]

    // Process header
    // ToDo: do we need a different license here
    result :+= Parts.header

    // Only output generation stamp for production environments
    if(!suppressTimestamps) {
      result ++= Stamp.Generate()
    }

    // Namespace
    //
    result :+= "package %s\n".format(root.header.namespace)

    // Process the imports
    //
    var subtitutions = new Substitution
    subtitutions.Add("model.name", root.header.namespace)
    subtitutions.Add("model.version", root.header.version)
    result :+= subtitutions.Run(Parts.imports)

    // Process additional imports like grok
    //
    val imports = if(root.grok.nonEmpty) {
                    root.imports.packages :+ "org.aicer.grok.dictionary.GrokDictionary"
                  } else {
                    root.imports.packages
                  }

    // Emit grok initialization
    val grokExpressions = if(root.grok.nonEmpty) {
      groks ++= BuildGrokInstance(root.grok.head._2)
      val (e, m) = BuildGrokCompiledExpression(root)
      groks ++= e
      m
    } else {
      Map.empty[String, (String, String, Set[String])]
    }

    // Append the packages needed
    //
    result ++= imports.distinct.map( i => "import %s".format(i) )

    // Add message so we can actual compile
    // Check how to reconcile during add/compilation
    //result ++= aliaseMessages.map(p => p._2).toSet.toArray.map( i => "import %s".format(i))

    // Collect all classes
    //
    val messagesSet = EvalTypes.CollectMessages(root)
    messagesSet.map( e => "%s aliases %s".format(e._1, e._2.mkString(", ")) ).foreach( m => {
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

    // Resolve dependencies from transformations
    //
    val dependencyToTransformations = EvalTypes.ResolveDependencies(root)

    // Upshot of the dependencies
    //
    dependencyToTransformations.map( e => {
      "Dependency [%s] => (%s)".format(e._1.mkString(", "), e._2._2.mkString(", "))
    }).foreach( m =>logger.trace(m) )

    // Create a map of dependency to id
    //
    val incomingToMsgId = dependencyToTransformations.foldLeft(Set.empty[String]) ( (r, e) => {
      r ++ e._1
    }).zipWithIndex.map( e => (e._1, e._2 + 1)).toMap

    // Return tru if we accept the message, flatten the messages into a list
    //
    val msgs = dependencyToTransformations.foldLeft(Set.empty[String]) ( (r, d) => {
      d._1.foldLeft(r) ((r, n) => {
        r ++ Set(n)
      })
    })

    subtitutions.Add("factory.isvalidmessage", msgs.map( m => {
      outmessages += m
      val verMsg = ResolveToVersionedClassname(md, m)
      "msg.isInstanceOf[%s]".format(verMsg)
    }).mkString("||") )

    val factory = subtitutions.Run(Parts.factory)
    result :+= factory

    // Generate variables
    //
    incomingToMsgId.foreach( e => {
      messages :+= "val msg%d = msgs.get(\"%s\").getOrElse(null).asInstanceOf[%s]".format(e._2, e._1, ResolveToVersionedClassname(md, e._1))
    })

    // Compute the highlevel handler that match dependencies
    //
    val handler = dependencyToTransformations.map( e => {

        // Trigger of incoming messages
        val check = e._1.map( m => { "msg%d!=null".format(incomingToMsgId.get(m).get)}).mkString(" && ")
        val names = e._1.map( m => { "msg%d".format(incomingToMsgId.get(m).get)}).mkString(", ")
        val depId = e._2._1
        val calls = e._2._2.map( f => "exeGenerated_%s_%d(%s)".format(f, depId, names) ).mkString(" ++ \n")
      """|if(%s) {
         |%s
         |} else {
         |  Array.empty[Result]
         |}
         |""".stripMargin('|').format(check, calls)
    })
    exechandler :+=  handler.mkString( "++\n")

    // Actual function to be called
    //
    dependencyToTransformations.foreach( e => {
      val deps = e._1
      val depId = e._2._1
      val transformationNames = e._2._2

      transformationNames.foreach( t => {

        val transformation = root.transformations.get(t).get
        val names = deps.map( m => { "msg%d: %s".format(incomingToMsgId.get(m).get, ResolveToVersionedClassname(md, m))}).mkString(", ")

        methods :+= transformation.Comment
        methods :+= "def exeGenerated_%s_%d(%s): Array[Result] = {".format(t, depId, names)

        // Collect form metadata
        val inputs: Array[Element] = ColumnNames(md, deps).map( e => {
          Element("msg%d".format(incomingToMsgId.get(e.className).get), e.className, e.fieldName)
        })

        // Resolve inputs, either we have unique or qualified names
        //
        val uniqueInputs: Map[String, String] = {
          val u = inputs.map( e => e.fieldName ).groupBy(identity).mapValues(_.length).filter( f => f._2==1).keys
          val u1 = u.map( e => inputs.find( c => c.fieldName == e).get)
          u1.map( p => p.fieldName -> "%s.%s".format(p.argName, p.fieldName))
        }.toMap

        val notUniqueInputs: Set[String] = {
          inputs.map( e => e.fieldName ).groupBy(identity).mapValues(_.length).filter( f => f._2>1).keys.toSet
        }

        val qualifiedInputs: Map[String, String]  = inputs.map( p => {
          p.className + "." + p.fieldName -> "%s.%s".format(p.argName, p.fieldName)
        }).toMap

        var fixedMappingSources = uniqueInputs ++ qualifiedInputs
        var trackedUsedSource : Set[String] = Set.empty[String]

        // Common computes section
        //
        var groks = transformation.grokMatch
        var computes = transformation.computes
        var cnt1 = computes.size + groks.size
        var cnt2 = -1

        while(cnt1!=cnt2 && (computes.nonEmpty || groks.nonEmpty)) {
          cnt2 = cnt1

          // Check grok matches
          //
          val groks1 = groks.filter( g => {

            // fs the input determined, emit as output expressions
            if(fixedMappingSources.contains(g._1)) {

              val nameColumn = ResolveName(g._1, aliaseMessages)

              trackedUsedSource += nameColumn

              // Get the expression
              //
              val d = grokExpressions.get(g._2).get

              logger.trace("Grok matched {} -> {}", nameColumn, d._3.mkString(", "))

              // The var name might generate conflicts
              // Let's be optimistic for now
              val varName = "%s_%s".format(d._1, g._1)
              methods :+= "lazy val %s = %s.extractNamedGroups(%s)".format(varName, d._1, nameColumn)

              // Emit variables w/ null value is needed
              // we are adding a complete expression here
              // potentially we have to decorate it to avoid naming conflicts
              d._3.foreach( e =>
                fixedMappingSources ++= Map(e -> "if(%s.containsKey(\"%s\")) %s.get(\"%s\") else \"\")".format(varName, e, varName, e))
              )
              false
            } else {
              //logger.trace("Grok not matched {}", g._1)
              true
            }
          })

          // Check computes
          val computes1 = computes.filter(c => {

            // Check if the compute if determined
            val (open, expression, list) =  if(c._2.expression.nonEmpty) {
              val list = Expressions.ExtractColumnNames(c._2.expression)
              val rlist = ResolveNames(list, aliaseMessages)
              val open = rlist.filter(f => !fixedMappingSources.contains(f._2))
              val ambiguous = rlist.filter(f => notUniqueInputs.contains(f._2)).map(m => m._2)
              if(ambiguous.nonEmpty) {
                val a = ambiguous.mkString(", ")
                logger.error("Found ambiguous variables %s in expression %s".format(a, c._2.expression))
                throw new Exception("Found ambiguous variables %s in expression %s".format(a, c._2.expression))
              }
              (open, c._2.expression, rlist)
            } else {
              val evaluate = c._2.expressions.map( expression => {
                val list = Expressions.ExtractColumnNames(expression)
                val rlist = ResolveNames(list, aliaseMessages)
                val open = rlist.filter(f => !fixedMappingSources.contains(f._2))
                val ambiguous = rlist.filter(f => notUniqueInputs.contains(f._2)).map(m => m._2)
                if(ambiguous.nonEmpty) {
                  val a = ambiguous.mkString(", ")
                  logger.error("Found ambiguous variables %s in expression %s".format(a, c._2.expression))
                  throw new Exception("Found ambiguous variables %s in expression %s".format(a, c._2.expression))
                }
                (open, expression, rlist)
              })
              evaluate.foldLeft(evaluate.head) ( (r, e) => {
                if(e._1.size < r._1.size)
                  e
                else
                  r
              })
            }

            if(open.isEmpty) {

              trackedUsedSource ++= list.map(m => m._2).toSet

              val newExpression = Expressions.FixupColumnNames(expression, fixedMappingSources, aliaseMessages)
              logger.trace("matched expression {} -> {}", newExpression, c._1)

              // Output the actual compute
              methods :+= c._2.Comment
              if(c._2.typename.length>0)
                methods ++= Array("val %s: %s = %s\n".format(c._1, c._2.typename, newExpression))
              else
                methods ++= Array("val %s = %s\n".format(c._1, newExpression))

              fixedMappingSources ++= Map(c._1 -> c._1)
              false
            } else {
              true
            }
          })

          cnt1 = computes1.size + groks.size
          computes = computes1
          groks = groks1
        }

        if(computes.nonEmpty){

          val c = computes.map(c => c._1).mkString(",")
          throw new Exception("Not all elements used. transformation: %s computes: %s".format(t, c))
          logger.trace("Not all elements used")
        }

        // Individual outputs
        //
        val inner = transformation.outputs.foldLeft(Array.empty[String]) ( (r, o) => {

          var collect = Array.empty[String]
          collect ++= Array("\ndef process_%s(): Array[Result] = {\n".format(o._1))

          val outputSet: Set[String] = ColumnNames(md, ResolveAlias(o._1, aliaseMessages))

          // State variables to track the progress
          // a little bit simpler than having val's
          var mappingSources: Map[String, String] = fixedMappingSources
          var trackedUsedSourceInner = trackedUsedSource

          logger.trace("Procesing transformation: {} output: {} outputs: {} mapping: {}",
            t, o._1, outputSet.mkString(","), mappingSources.mkString(","))

          var outputSet1: Set[String] = outputSet

          // Go through the inputs and find the system column so we can just funnel it through
          columnSystem.foreach( c => {
            if(outputSet1.contains(c)) {
              val i = inputs.find( f => f.fieldName == c)
              outputSet1 --= Set(c)
              val f = "%s.%s".format(i.get.argName, i.get.fieldName)
              mappingSources ++= Map(c -> f)
              trackedUsedSourceInner += f
            }
          })

          var mapping = o._2.mapping
          var wheres =  if(o._2.where.nonEmpty) Array(o._2.where) else Array.empty[String]
          var computes = o._2.computes
          var cnt1 = wheres.length + computes.size
          var cnt2 = -1

          // Abort this loop if nothing changes or we can satisfy all outputs
          while(cnt1!=cnt2 && outputSet1.nonEmpty) {

            cnt2 = cnt1

            // Check Mapping
            if(mapping.nonEmpty)
            {
              logger.trace("Mappings left {}", mapping.mkString(", "))

              val found = mapping.filter( f => {
                // Try to extract variables, than it is an expression
                val list = Expressions.ExtractColumnNames(f._2)
                if(list.nonEmpty) {
                  val rList = ResolveNames(list, root.aliases.messages.toMap)
                  val open = rList.filter(f => !mappingSources.contains(f._2))
                  if(open.nonEmpty) logger.trace("{} not found {}", t.toString, open.mkString(", "))
                  open.isEmpty
                } else {
                  mappingSources.contains(f._2)
                }
              })

              found.foreach(f => {
                // Try to extract variables, than it is an expression
                val expression = f._2
                val list = Expressions.ExtractColumnNames(expression)

                val newExpression = if (list.nonEmpty) {
                  val newExpression = Expressions.FixupColumnNames(expression, mappingSources, aliaseMessages)
                  val rList = ResolveNames(list, root.aliases.messages.toMap)
                  val open = rList.filter(f => !mappingSources.contains(f._2))
                  logger.trace("Matched mapping expression {} -> {}", f._1, newExpression)
                  trackedUsedSourceInner ++= rList.map(m => m._2).toSet
                  newExpression
                } else {
                  val nameColumn = ResolveName(expression, aliaseMessages)
                  trackedUsedSourceInner += nameColumn
                  nameColumn
                }
                outputSet1 --= Set(f._1)
                mappingSources ++= Map(f._1 -> newExpression)
              })

              mapping = mapping.filterKeys( f => !found.contains(f)  )
            }

            // Check grok matches

            // filters
            val wheres1 = wheres.filter(f => {
              val list = Expressions.ExtractColumnNames(f)
              val rList = ResolveNames(list, root.aliases.messages.toMap)
              val open = rList.filter(f => !mappingSources.contains(f._2) )
              val ambiguous = rList.filter(f => notUniqueInputs.contains(f._2)).map(m => m._2)
              if(ambiguous.nonEmpty) {
                val a = ambiguous.mkString(", ")
                logger.error("Found ambiguous variables %s in expression %s".format(a, f))
                throw new Exception("Found ambiguous variables %s in expression %s".format(a, f))
              }

              if(open.isEmpty) {

                trackedUsedSourceInner ++= rList.map(m => m._2).toSet

                // Sub names to
                val newExpression = Expressions.FixupColumnNames(f, mappingSources, aliaseMessages)
                logger.trace("Matched where expression {}", newExpression)
                // Output the actual filter
                collect ++= Array("if (%s) return Array.empty[Result]\n".format(newExpression))
                false
              } else {
                true
              }
            })

            // computes
            val computes1 = computes.filter( c => {

              // Check if the compute if determind
              val (open, expression, list) =  if(c._2.expression.length > 0) {
                val list = Expressions.ExtractColumnNames(c._2.expression)
                val rList = ResolveNames(list, root.aliases.messages.toMap)
                val open = rList.filter(f => !mappingSources.contains(f._2))
                val ambiguous = rList.filter(f => notUniqueInputs.contains(f._2)).map(m => m._2)
                if(ambiguous.nonEmpty) {
                  val a = ambiguous.mkString(", ")
                  logger.error("Found ambiguous variables %s in expression %s".format(a, c._2.expression))
                  throw new Exception("Found ambiguous variables %s in expression %s".format(a, c._2.expression))
                }
                (open, c._2.expression, rList)
              } else {
                val evaluate = c._2.expressions.map( expression => {
                  val list = Expressions.ExtractColumnNames(expression)
                  val rList = ResolveNames(list, root.aliases.messages.toMap)
                  val open = rList.filter(f => !mappingSources.contains(f._2))
                  val ambiguous = rList.filter(f => notUniqueInputs.contains(f._2)).map(m => m._2)
                  if(ambiguous.nonEmpty) {
                    val a = ambiguous.mkString(", ")
                    logger.error("Found ambiguous variables %s in expression %s".format(a, c._2.expression))
                    throw new Exception("Found ambiguous variables %s in expression %s".format(a, c._2.expression))
                  }
                  (open, expression, rList)
                })
                evaluate.foldLeft(evaluate.head) ( (r, e) => {
                  if(e._1.size < r._1.size)
                    e
                  else
                    r
                })
              }

              if(open.isEmpty) {
                trackedUsedSourceInner ++= list.map(m => m._2).toSet
                // Sub names to
                val newExpression = Expressions.FixupColumnNames(expression, mappingSources, aliaseMessages)
                logger.trace("Matched compute expression {} -> {}", newExpression, c._1)
                // Output the actual compute
                // To Do: multiple vals and type provided
                collect :+= c._2.Comment
                if(c._2.typename.length>0)
                  collect ++= Array("val %s: %s = %s\n".format(c._1, c._2.typename, newExpression))
                else
                  collect ++= Array("val %s = %s\n".format(c._1, newExpression))

                outputSet1 --= Set(c._1)
                mappingSources ++= Map(c._1 -> c._1)
                false
              } else {
                true
              }
            })

            // Update state
            cnt1 = wheres1.length + computes1.size
            wheres = wheres1
            computes = computes1
          }

          if(computes.nonEmpty){
            val c = computes.map(c => c._1).mkString(",")
            val m = "Not all elements used. transformation: %s computes: %s".format(t, c)
            logger.trace(m)
          }

          if(outputSet1.nonEmpty){
            val m = "Not all outputs satisfied. transformation: %s output: %s missing: %s".format(t, o._1, outputSet1.mkString(", "))
            logger.trace(m)
            throw new Exception(m)
          }

          if(cnt2!=0){
            logger.trace("Not all elements used")
            //throw new Exception("Not all elements used")
          }

          // Generate the output for this iteration
          // Translate outputs to the values
          val outputElements = outputSet.map( e => {
            // e.name -> from input, from mapping, from variable
            val m = mappingSources.get(e)
            if(m.isEmpty) {
              throw new Exception("Output %s not found".format(e))
            }
            "new Result(\"%s\", %s)".format(e, m.get)
          }).mkString(", ")

          // To Do: this is not correct
          val outputResult = "Array[Result](%s)".format(outputElements)

          collect ++= Array(outputResult)
          collect ++= Array("}\n")

          // Collect all input messages attribute used
          logger.trace("Final map: transformation %s ouput %s used %s".format(t, o._1, trackedUsedSourceInner.mkString(", ")))

          // outputs
          r ++ collect
        })

        methods ++= inner

        // Output the function calls
        methods :+= transformation.outputs.map( o => {
          "process_%s()".format(o._1)
        }).mkString("++\n")

        methods :+= "}"

      })
    })

    val resultVar = "val results: Array[Result] = \n"
    val returnValue = "factory.createResultObject().asInstanceOf[MappedModelResults].withResults(results)"
    subtitutions.Add("model.grok", groks.mkString("\n"))
    subtitutions.Add("model.message", messages.mkString("\n"))
    subtitutions.Add("model.methods", methods.mkString("\n"))
    subtitutions.Add("model.code", resultVar + "\n" + exechandler.mkString("\n") + "\n" + returnValue + "\n")
    val model = subtitutions.Run(Parts.model)
    result :+= model

    // Write to output file
    // Store a copy in the object
    code = CodeHelper.Indent(result)

    if(outputFile!=null && outputFile.nonEmpty) {
      logger.trace("Output to file {}", outputFile)
      FileUtils.writeStringToFile(new File(outputFile), code)
    }

    code
  }
}
