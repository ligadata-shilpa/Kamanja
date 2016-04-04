package com.ligadata.MetadataAPI.Utility

import com.ligadata.Exceptions.{InvalidArgumentException, EngineConfigParsingException, Json4sParsingException}
import org.apache.logging.log4j.LogManager
import org.json4s.jackson.JsonMethods._
import org.json4s.{MappingException, DefaultFormats, Formats}

import scala.collection.mutable.Map
import scala.io.Source

object AdapterMessageBindingService {
    val loggerName = this.getClass.getName
    lazy val logger = LogManager.getLogger(loggerName)

    /**
      * Add an adapter message binding to the metadata
      *
      * @param input the binding specification, either in the form of a JSON string specifying the binding
      *              or
      *              a path specification of a file that contains the binding(s).
      * @param userId the user that is performing the add (currently optional)
      * @return a JSON message result
      */
    def addAdapterMessageBinding(input: String, userId : Option[String]) : String = {
        val isInlineSpec : Boolean = (input != null && (input.trim.startsWith("{") || input.trim.startsWith("[")))
        val result : String = if (isInlineSpec) {
            addAdapterMessageBindingFromJson(input.trim, userId)
        } else {
            val jsonText: String = Source.fromFile(input).mkString
            addAdapterMessageBindingFromJson(jsonText, userId)
        }
        result
    }

    /**
      * Process the supplied json string.  One (starts with '{') or more (starts with ']') adapters may be present
      *
      * @param input
      * @param userId
      * @return
      */
    def addAdapterMessageBindingFromJson(input: String, userId : Option[String]) : String = {
        val isMap : Boolean = (input != null && input.trim.startsWith("{"))
        val isList : Boolean = (input != null && input.trim.startsWith("["))

        val reasonable : Boolean = isMap || isList
        if (! reasonable) {
            throw new IllegalArgumentException("the adapter string specified must be either a json map or json array.")
        }
        val result : String = if (isMap) {
            val bindingSpec : Map[String,Any] = jsonStringAsColl(input).asInstanceOf[Map[String,Any]]

            ""
        } else {
            val bindingSpecList : List[Map[String,Any]] = jsonStringAsColl(input).asInstanceOf[List[Map[String,Any]]]

            ""
        }

        result
    }

    /**
      * Translate the supplied json string to a Map[String, Any]
      *
      * @param configJson
      * @return Map[String, Any]
      */

    @throws(classOf[com.ligadata.Exceptions.Json4sParsingException])
    @throws(classOf[com.ligadata.Exceptions.InvalidArgumentException])
    def jsonStringAsMap(configJson: String): Map[String, Any] = {
        try {
            implicit val jsonFormats: Formats = DefaultFormats
            val json = parse(configJson)
            logger.debug("Parsed the json : " + configJson)

            val fullmap = json.values.asInstanceOf[Map[String, Any]]

            fullmap
        } catch {
            case e: MappingException => {
                logger.debug("", e)
                throw Json4sParsingException(e.getMessage, e)
            }
            case e: Exception => {
                logger.debug("", e)
                throw InvalidArgumentException(e.getMessage, e)
            }
        }
    }

    /**
      * Translate the supplied json string to a List[Map[String, Any]]
      *
      * @param configJson
      * @return Map[String, Any]
      */

    @throws(classOf[com.ligadata.Exceptions.Json4sParsingException])
    @throws(classOf[com.ligadata.Exceptions.InvalidArgumentException])
    def jsonStringAsColl(configJson: String): Any = {
        val jsonObjs : Any = try {
            implicit val jsonFormats: Formats = DefaultFormats
            val json = parse(configJson)
            logger.debug("Parsed the json : " + configJson)

            json
        } catch {
            case e: MappingException => {
                logger.debug("", e)
                throw Json4sParsingException(e.getMessage, e)
            }
            case e: Exception => {
                logger.debug("", e)
                throw InvalidArgumentException(e.getMessage, e)
            }
        }
        jsonObjs
    }

    /**
      * Update an existing adapter message binding to the metadata.
      *
      * @param input the binding specification, either in the form of a JSON string specifying the binding
      *              or
      *              a path specification of a file that contains the binding(s).
      * @param userId the user that is performing the update (currently optional)
      * @return a JSON message result
      */
    def updateAdapterMessageBinding(input: String, userId : Option[String]) : String  = {

        ""
    }

    /**
      * Remove an existing adapter message binding from the metadata.
      *
      * @param input the name of the binding to remove.  This name is a namespace qualified name.
      * @param userId the user that is performing the remove (currently optional)
      * @return a JSON message result
      */
    def removeAdapterMessageBinding(input: String, userId : Option[String]) : String  = {

        ""
    }

}
