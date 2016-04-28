package com.ligadata.tools.containersutility

/**
  * Created by Yousef on 3/9/2016.
  */

import java.io.{FileOutputStream, OutputStream, PrintWriter}
import java.util.zip.GZIPOutputStream
import com.ligadata.KvBase.TimeRange

import scala.collection.mutable._
import org.apache.logging.log4j.LogManager
import com.ligadata.Utils.Utils
import com.ligadata.MetadataAPI.MetadataAPIImpl
import com.ligadata.Exceptions._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import scala.collection.immutable.Map
import java.text.SimpleDateFormat

trait LogTrait {
  val loggerName = this.getClass.getName()
  val logger = LogManager.getLogger(loggerName)
}

case class containeropt(begintime: Option[String], endtime: Option[String], keys: Option[Array[Array[String]]])

case class container(begintime: String, endtime: String, keys: Array[Array[String]])

object ContainersUtility extends App with LogTrait {

  case class data(key: String, value: String)

  def usage: String = {
    """
Usage: scala com.ligadata.containersutility.ContainersUtility
    --config <config file while has jarpaths, metadata store information & data store information>
    --containername <full package qualified name of a Container>
    --keyfieldname  <name of key for container>
    --operation <truncate, select, delete>
    --keyid <key ids for select or delete>
Sample uses:
      java -jar /tmp/KamanjaInstall/ContainersUtility-1.0 --containername System.TestContainer --config /tmp/KamanjaInstall/EngineConfig.cfg --keyfieldname Id --oepration truncate
    """
  }

  def writeToFile(data: Map[String, String], outputPath: String, containerName: String): Unit = {
    if (outputPath.equalsIgnoreCase(null)) {
      logger.error("outputpath should not be null for select operation")
    } else {
      if (!data.isEmpty) {
        val dateFormat = new SimpleDateFormat("ddMMyyyyhhmmss")
        val filename = outputPath + "/result_" + dateFormat.format(new java.util.Date()) + ".json"
        val json = ("container name" -> containerName) ~
          ("data" ->
            data.keys.map {
              key =>
                (
                  ("key" -> key) ~
                    ("value" -> data(key)))
            })
        new PrintWriter(filename) {
          write(pretty(render(json)));
          close
        }
      } else {
        logger.error("no data retrieved")
      }
    }
  }

  override def main(args: Array[String]) {

    logger.debug("ContainersUtility.main begins")
    implicit val formats = DefaultFormats
    if (args.length == 0) logger.error(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, String]
    logger.debug(arglist)
    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--config" :: value :: tail =>
          nextOption(map ++ Map('config -> value), tail)
        case "--containername" :: value :: tail =>
          nextOption(map ++ Map('containername -> value), tail)
        case "--operation" :: value :: tail =>
          nextOption(map ++ Map('operation -> value), tail)
        case "--keyfields" :: value :: tail =>
          nextOption(map ++ Map('keyfields -> value), tail)
        case "--outputpath" :: value :: tail =>
          nextOption(map ++ Map('outputpath -> value), tail)
        case "--filter" :: value :: tail =>
          nextOption(map ++ Map('filter -> value), tail)
        case "--serializer" :: value :: tail =>
          nextOption(map ++ Map('serializer -> value), tail)
        case "--serializeroptionsjson" :: value :: tail =>
          nextOption(map ++ Map('serializeroptionsjson -> value), tail)
        case "--compressionstring" :: value :: tail =>
          nextOption(map ++ Map('compressionstring -> value), tail)
        case option :: tail =>
          logger.error("Unknown option " + option)
          sys.exit(1)
      }
    }

    val options = nextOption(Map(), arglist)

    var cfgfile = if (options.contains('config)) options.apply('config) else null // datatore name and connection string
    var containerName = if (options.contains('containername)) options.apply('containername) else null // container name
    var operation = if (options.contains('operation)) options.apply('operation) else "" // operation select/truncate/delete
    val tmpkeyfieldnames = if (options.contains('keyfields)) options.apply('keyfields) else null //key field name
    val output = if (options.contains('outputpath)) options.apply('outputpath) else null //output path for select operation
    val filter = if (options.contains('filter)) options.apply('filter) else "" // include keyid and timeranges
    val serializerName = if (options.contains('serializer)) options.apply('serializer) else "" // include serializer name
    val serializerOptionsJson = if (options.contains('serializeroptionsjson)) options.apply('serializeroptionsjson) else null
    val compressionString = if (options.contains('compressionstring)) options.apply('compressionstring) else null
    var containerObj: List[container] = null

    if(operation.equals("")|| (!operation.equalsIgnoreCase("truncate") && !operation.equalsIgnoreCase("select") && !operation.equalsIgnoreCase("delete"))){//check if a correct operation passed or not
      logger.error("you should pass truncate or delete or select in operation option")
      sys.exit(1)
    }
    if (!operation.equalsIgnoreCase("truncate")) { // check if operation does not match truncate because in truncate we do not need to parse filter file

      if(filter.equals("")){ // if user does not pass a file that includes keys and/or timeranges
        logger.error("you should pass a filter file which includes keys and/or timeranges in filter option")
        sys.exit(1)
      } else if(new java.io.File(filter).exists.equals(false)){ // check if path exits or not for filter file
        logger.error("this path does not exist: %s".format(filter))
        sys.exit(1)
      }

      val filterFile = scala.io.Source.fromFile(filter).mkString // read filter file config (JSON file)
      val parsedKey = parse(filterFile)
      if (parsedKey != null) { // check if there is data inside filter file or not
        val optContainerObj = parsedKey.extract[List[containeropt]]
        containerObj = optContainerObj.map(c => {
          if (c.begintime == None && c.endtime == None && c.keys == None) { //use to check if user add data in filter file
            logger.error("you should pass time range or key(s) or both")
            sys.exit(1)
          }
          val bt = if (c.begintime != None) c.begintime.get else Long.MinValue.toString
          val et = if (c.endtime != None) c.endtime.get else Long.MaxValue.toString
          val keys = if (c.keys != None) c.keys.get else Array[Array[String]]()
          container(bt, et, keys)
        })
      } else {
        logger.error("you should pass a filter file includes keys and/or timesrange for select and delete operation")
        sys.exit(1)
      }

      if(operation.equalsIgnoreCase("select")) {
        if (serializerName.equals("")) { // check if user pass serializer option for select operation
          logger.error("you should pass a serializer option for select operation")
          sys.exit(1)
        }

//        if (serializerOptionsJson.equals(null)) { // check if user pass serializerOptionsJson for select operation
//          logger.error("you should pass a serializeroptionsjson option for select operation")
//          sys.exit(1)
//        }

//        if (compressionString.equals(null)) {// check if user pass compressionString for select operation
//          logger.error("you should pass a compressionString option for select operation")
//        }
      }
    }

    var valid: Boolean = (cfgfile != null && containerName != null)

    if (valid) {
      cfgfile = cfgfile.trim
      containerName = containerName.trim
      valid = (cfgfile.size != 0 && containerName.size != 0)
    }

    if (valid) {
      val (loadConfigs, failStr) = Utils.loadConfiguration(cfgfile.toString, true)
      if (failStr != null && failStr.size > 0) {
        logger.error(failStr)
        sys.exit(1)
      }
      if (loadConfigs == null) {
        logger.error("Failed to load configurations from configuration file")
        sys.exit(1)
      }

      containersUtilityConfiguration.configFile = cfgfile.toString

      val utilmaker: UtilityForContainers = new UtilityForContainers(loadConfigs, containerName.toLowerCase)
      if (utilmaker.isOk) {
        try {
          val dstore = utilmaker.GetDataStoreHandle(containersUtilityConfiguration.jarPaths, utilmaker.dataDataStoreInfo)
          if (dstore != null) {
            try {
              dstore.setObjectResolver(utilmaker)
//              if(!dstore.isTableExists(containerName)){
//                logger.error("there is no %s container in datastore".format(containerName))
//              }
              if (!operation.equals("")) {
                if (operation.equalsIgnoreCase("truncate")) {
                  utilmaker.TruncateContainer(containerName, dstore)
                } else if (operation.equalsIgnoreCase("delete")) {
                  if (containerObj.size == 0)
                    logger.error("Failed to delete data from %s container, at least one item (keyid, timerange) should not be null for delete operation".format(containerName))
                  else
                    utilmaker.DeleteFromContainer(containerName, containerObj, dstore)
                } else if (operation.equalsIgnoreCase("select")) {
                  dstore.setDefaultSerializerDeserializer(serializerName, scala.collection.immutable.Map[String, Any]())
                  if (containerObj.size == 0)
                    logger.error("Failed to select data from %s container,at least one item (keyid, timerange) should not be null for select operation".format(containerName))
                  else {
//                    val serializerName: String = "com.ligadata.kamanja.serializer.jsonserdeser" // BUGBUG:: FIXME: Get this from input option
//                    val serializerOptionsjson: String = "" // BUGBUG:: FIXME: Get this from input option
//                    val compressionString: String = "" // BUGBUG:: FIXME: Get this from input option
                    if(new java.io.File(output).exists.equals(false)){ // check if path exits or not for filter file
                      logger.error("this path does not exist: %s".format(output))
                      sys.exit(1)
                    }
                    val dateFormat = new SimpleDateFormat("ddMMyyyyhhmmss")
                    val contInFl = containerName.trim.replace(".", "_").replace("\\", "_").replace("/", "_")
                    val filename = output + "/" + contInFl + "_result_" + dateFormat.format(new java.util.Date()) + ".dat"

                    var os: OutputStream = null

                    try {
                      if (compressionString == null || compressionString.trim.size == 0) {
                        os = new FileOutputStream(filename);
                      } else if (compressionString.trim.compareToIgnoreCase("gz") == 0) {
                        os = new GZIPOutputStream(new FileOutputStream(filename))
                      } else {
                        throw new Exception("Compression %s is not yet handled. We support only uncompressed file & GZ files".format(compressionString))
                      }

                      val ln = "\n".getBytes("UTF8")

                      val getData = (data: Array[Byte]) => {
                        if (data != null) {
                          os.write(data)
                          os.write(ln)
                        }
                      }
                      utilmaker.GetFromContainer(containerName, containerObj, dstore, serializerName, serializerOptionsJson, getData)
                    } catch {
                      case e: Exception => {
                        logger.error("Failed to select data", e)
                        throw e
                      }
                    } finally {
                      if (os != null)
                        os.close
                      os = null
                    }
                  }
                }
              } else {
                logger.error("Unknown operation you should use one of these options: select, delete, truncate")
              }
            } catch {
              case e: Exception => {
                logger.error("Failed to build Container or Message.", e)
              }
            } finally {
              if (dstore != null)
                dstore.Shutdown()
              com.ligadata.transactions.NodeLevelTransService.Shutdown
              if (utilmaker.zkcForSetData != null)
                utilmaker.zkcForSetData.close()
            }
          }
        } catch {
          case e: FatalAdapterException => {
            logger.error("Failed to connect to Datastore.", e)
          }
          case e: StorageConnectionException => {
            logger.error("Failed to connect to Datastore.", e)
          }
          case e: StorageFetchException => {
            logger.error("Failed to connect to Datastore.", e)
          }
          case e: StorageDMLException => {
            logger.error("Failed to connect to Datastore.", e)
          }
          case e: StorageDDLException => {
            logger.error("Failed to connect to Datastore.", e)
          }
          case e: Exception => {
            logger.error("Failed to connect to Datastore.", e)
          }
          case e: Throwable => {
            logger.error("Failed to connect to Datastore.", e)
          }
        }
      }
      MetadataAPIImpl.CloseDbStore

    } else {
      logger.error("Illegal and/or missing arguments")
      logger.error(usage)
    }
  }
}

object containersUtilityConfiguration {
  var nodeId: Int = _
  var configFile: String = _
  var jarPaths: collection.immutable.Set[String] = _
}