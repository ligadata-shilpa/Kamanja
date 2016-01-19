package com.ligadata.jtm

import org.apache.logging.log4j.{ Logger, LogManager }

import org.rogach.scallop._

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

/* Translates a jtm (json) file(s) into scala classes
 *
 */
class Compiler extends LogTrait {

}