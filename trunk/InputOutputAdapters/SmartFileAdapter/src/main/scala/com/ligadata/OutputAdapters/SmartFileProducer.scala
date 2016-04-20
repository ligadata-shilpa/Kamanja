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

package com.ligadata.OutputAdapters

import org.apache.logging.log4j.{ Logger, LogManager }
import java.io._
import java.text.SimpleDateFormat
import java.util.zip.{ ZipException, GZIPOutputStream }
import java.nio.file.{ Paths, Files }
import java.net.URI
import com.ligadata.KamanjaBase.{ContainerInterface, TransactionContext, NodeContext}
import com.ligadata.InputOutputAdapterInfo._
import com.ligadata.AdaptersConfiguration.SmartFileAdapterConfiguration
import com.ligadata.Exceptions.{ FatalAdapterException }
import com.ligadata.HeartBeat.{ Monitorable, MonitorComponentInfo }
import org.json4s.jackson.Serialization
import org.apache.hadoop.fs.{ FileSystem, FSDataOutputStream, Path }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.commons.compress.compressors.CompressorStreamFactory

object SmartFileProducer extends OutputAdapterFactory {
  val ADAPTER_DESCRIPTION = "Smart File Output Adapter"
  def CreateOutputAdapter(inputConfig: AdapterConfiguration, nodeContext: NodeContext): OutputAdapter = new SmartFileProducer(inputConfig, nodeContext)
}

class SmartFileProducer(val inputConfig: AdapterConfiguration, nodeContext: NodeContext) extends OutputAdapter {
  private[this] val _lock = new Object()
  private[this] val LOG = LogManager.getLogger(getClass);

  private[this] val fc = SmartFileAdapterConfiguration.getAdapterConfig(inputConfig)
  private var ugi: UserGroupInformation = null
  private var partitionStreams: collection.mutable.Map[String,OutputStream] = collection.mutable.Map[String,OutputStream]()
  
  private val nodeId = nodeContext.getEnvCtxt().getNodeId()
  private val FAIL_WAIT = 2000
  private var numOfRetries = 0
  private var MAX_RETRIES = 3
  private var startTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(System.currentTimeMillis))
  private var lastSeen = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(System.currentTimeMillis))
  private var metrics: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()

  // validate the configuration values
  if (fc._type == null || !"file".equalsIgnoreCase(fc._type) || !"hdfs".equalsIgnoreCase(fc._type))
    throw FatalAdapterException("Type should be file or hdfs for Smart File Producer: " + fc.Name, new Exception("Invalid Parameters"))
  
  if (fc.producerConfig == null)
    throw FatalAdapterException("ProducerConfig should be specified for Smart File Producer: " + fc.Name, new Exception("Invalid Parameters"))

  if (fc.producerConfig.location == null || fc.producerConfig.location.size == 0)
    throw FatalAdapterException("Location should not be NULL or empty for Smart File Producer", new Exception("Invalid Parameters"))

  if (fc.connectionConfig == null && "hdfs".equalsIgnoreCase(fc._type))
    throw FatalAdapterException("ConnectionConfig should be specified for Smart File Producer: " + fc.Name, new Exception("Invalid Parameters"))

  val kerberosEnabled = ("hdfs".equalsIgnoreCase(fc._type) && fc.connectionConfig.authentication != null && fc.connectionConfig.authentication.equalsIgnoreCase("kerberos"))
  if (kerberosEnabled) {
    LOG.debug("Kerberos security authentication enabled.")
    if (fc.connectionConfig.principal == null || fc.connectionConfig.principal.size == 0)
      throw FatalAdapterException("Principal should be specified for Kerberos authentication", new Exception("Invalid Parameters"))
    LOG.debug("Using Kerberos principal: " + fc.connectionConfig.principal)

    if (fc.connectionConfig.keytab == null || fc.connectionConfig.keytab.size == 0)
      throw FatalAdapterException("Keytab should be specified for Kerberos authentication", new Exception("Invalid Parameters"))
    LOG.debug("Using Kerberos keytab file: " + fc.connectionConfig.keytab)
  }

  val compress = (fc.producerConfig.compressionString != null)
  if (compress) {
    if (CompressorStreamFactory.BZIP2.equalsIgnoreCase(fc.producerConfig.compressionString) ||
      CompressorStreamFactory.GZIP.equalsIgnoreCase(fc.producerConfig.compressionString) ||
      CompressorStreamFactory.XZ.equalsIgnoreCase(fc.producerConfig.compressionString) ||
      CompressorStreamFactory.PACK200.equalsIgnoreCase(fc.producerConfig.compressionString))
      //CompressorStreamFactory.DEFLATE.equalsIgnoreCase(fc.CompressionString))
      LOG.debug("Using compression: " + fc.producerConfig.compressionString)
    else
      throw FatalAdapterException("Unsupported compression type " + fc.producerConfig.compressionString, new Exception("Invalid Parameters"))
  }

  val partitionVariable = "\\$\\{([^\\}]+)\\}".r
  val partitionDateFormats = partitionVariable.findAllMatchIn(fc.producerConfig.partitionFormat).map(x => try {
      new SimpleDateFormat(x.group(1))
    } catch {
      case e: Exception => { throw FatalAdapterException(x.group(1) + " is not a valid date format string.", e)}
    }).toList
  val partitionFormatString = partitionVariable.replaceAllIn(fc.producerConfig.partitionFormat, "%s")
  
  private def openFile(fileName: String) = if("hdfs".equalsIgnoreCase(fc._type)) openHdfsFile(fileName) else openFsFile(fileName)

  private def openFsFile(fileName: String): OutputStream = {
    var os: OutputStream = null
    while (os == null) {
      try {
        os = new FileOutputStream(fileName, true) 
      } catch {
        case fio: IOException => {
          LOG.warn("Smart File Producer " + fc.Name + ": Unable to create a file destination " + fc.producerConfig.location + " due to an IOException", fio)
          if (numOfRetries > MAX_RETRIES) {
            LOG.error("Smart File Producer " + fc.Name + ":Unable to create a file destination after " + MAX_RETRIES + " tries.  Aborting.")
            throw FatalAdapterException("Unable to open connection to specified file after " + MAX_RETRIES + " retries", fio)
          }
          numOfRetries += 1
          LOG.warn("Smart File Producer " + fc.Name + ": Retyring " + numOfRetries + "/" + MAX_RETRIES)
          Thread.sleep(FAIL_WAIT)
        }
        case e: Exception => { throw FatalAdapterException("Unable to open connection to specified file ", e) }
      }
    }
    return os;
  }

  private def openHdfsFile(fileName: String): OutputStream = {
    var os: OutputStream = null
    while (os == null) {
      try {
        var hdfsConf: Configuration = new Configuration();
        if (kerberosEnabled) {
          hdfsConf.set("hadoop.security.authentication", "kerberos")
          UserGroupInformation.setConfiguration(hdfsConf)
          ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(fc.connectionConfig.principal, fc.connectionConfig.keytab);
        }

        var uri: URI = URI.create(fileName)
        var path: Path = new Path(uri)
        var fs: FileSystem = FileSystem.get(uri, hdfsConf);

        if (fs.exists(path)) {
          LOG.info("Loading existing file " + uri)
          os = fs.append(path)
        } else {
          LOG.info("Creating new file " + uri);
          os = fs.create(path);
        }
      } catch {
        case fio: IOException => {
          LOG.warn("Smart File Producer " + fc.Name + ": Unable to create a file destination " + fc.producerConfig.location + " due to an IOException", fio)
          if (numOfRetries > MAX_RETRIES) {
            LOG.error("Smart File Producer " + fc.Name + ":Unable to create a file destination after " + MAX_RETRIES + " tries.  Aborting.")
            throw FatalAdapterException("Unable to open connection to specified file after " + MAX_RETRIES + " retries", fio)
          }
          numOfRetries += 1
          LOG.warn("Smart File Producer " + fc.Name + ": Retyring " + numOfRetries + "/" + MAX_RETRIES)
          Thread.sleep(FAIL_WAIT)
        }
        case e: Exception => { throw FatalAdapterException("Unable to open connection to specified file ", e) }
      }
    }
    return os;
  }
  
  override def getComponentStatusAndMetrics: MonitorComponentInfo = {
    implicit val formats = org.json4s.DefaultFormats
    return new MonitorComponentInfo(AdapterConfiguration.TYPE_OUTPUT, fc.Name, SmartFileProducer.ADAPTER_DESCRIPTION, startTime, lastSeen, Serialization.write(metrics).toString)
  }

  private def getOutputStream(record: ContainerInterface) : OutputStream = {
    val dateTime = record.getTimePartitionData()
    val values = partitionDateFormats.map(fmt => fmt.format(dateTime))
    val key = record.getTypeName() + "/" + partitionFormatString.format(values: _*)
    if(!partitionStreams.contains(key)) {
      val fileName = "%s/%s/%s-%d-%d.dat".format(fc.producerConfig.location, key, fc.producerConfig.fileNamePrefix, nodeId, System.currentTimeMillis())
      var os = openFile(fileName)
      if (compress)
        os = new CompressorStreamFactory().createCompressorOutputStream(fc.producerConfig.compressionString, os)
        
      partitionStreams(key) = os
    }
    
    return partitionStreams(key)
  }
  
  // Locking before we write into file
  // To send an array of messages. messages.size should be same as partKeys.size
  protected override def send(tnxCtxt: TransactionContext, outputContainers: Array[ContainerInterface], serializedContainerData: Array[Array[Byte]], serializerNames: Array[String]): Unit = _lock.synchronized {
    if (outputContainers.size != serializedContainerData.size || outputContainers.size != serializerNames.size) {
      LOG.error("Smart File Producer " + fc.Name + ": Messages, messages serialized data & serializer names should has same number of elements. Messages:%d, Messages Serialized data:%d, serializerNames:%d".format(outputContainers.size, serializedContainerData.size, serializerNames.size))
      //TODO Need to record an error here... is this a job for the ERROR Q?
      return
    }
    if (serializedContainerData.size == 0) return

    try {
      // Op is not atomic
      (serializedContainerData, outputContainers).zipped.foreach((message, record) => {
        var isSuccess = false
        numOfRetries = 0
        while (!isSuccess) {
          try {
            val os = getOutputStream(record);
            os.write(message);
            isSuccess = true
            LOG.debug("finished writing message")
          } catch {
            case fio: IOException => {
              LOG.warn("Smart File Producer " + fc.Name + ": Unable to write to file " + fc.producerConfig.location)
              if (numOfRetries >= MAX_RETRIES) {
                LOG.error("Smart File Producer " + fc.Name + ": Unable to create a file destination after " + MAX_RETRIES + " tries.  Aborting.", fio)
                throw FatalAdapterException("Unable to open connection to specified file after " + MAX_RETRIES + " retries", fio)
              }
              numOfRetries += 1
              LOG.warn("Smart File Producer " + fc.Name + ": Retyring " + numOfRetries + "/" + MAX_RETRIES)
              Thread.sleep(FAIL_WAIT)
            }
            case e: Exception => {
              LOG.error("Smart File Producer " + fc.Name + ": Unable to write output message: " + new String(message), e)
              throw e
            }
          }
        }
      })
    } catch {
      case e: Exception => {
        LOG.error("Smart File Producer " + fc.Name + ": Failed to send", e)
        throw FatalAdapterException("Unable to send message", e)
      }
    }
  }

  override def Shutdown(): Unit = _lock.synchronized {
    if (os != null) {
      LOG.debug("closing file")
      os.close
    }
  }

}

