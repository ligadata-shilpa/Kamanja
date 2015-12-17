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

package com.ligadata.InputAdapters

import scala.actors.threadpool.{ Executors, ExecutorService }
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import org.apache.logging.log4j.{ Logger, LogManager }
import com.ligadata.InputOutputAdapterInfo.{ AdapterConfiguration, InputAdapter, InputAdapterObj, OutputAdapter, ExecContext, ExecContextObj, CountersAdapter, PartitionUniqueRecordKey, PartitionUniqueRecordValue, StartProcPartInfo, InputAdapterCallerContext }
import com.ligadata.AdaptersConfiguration.{ IbmMqAdapterConfiguration, IbmMqPartitionUniqueRecordKey, IbmMqPartitionUniqueRecordValue }
import javax.jms.{ Connection, Destination, JMSException, Message, MessageConsumer, Session, TextMessage, BytesMessage }
import scala.util.control.Breaks._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.ibm.msg.client.jms.JmsConnectionFactory
import com.ibm.msg.client.jms.JmsFactoryFactory
import com.ibm.msg.client.wmq.WMQConstants
import com.ibm.msg.client.wmq.common.CommonConstants
import com.ibm.msg.client.jms.JmsConstants
import com.ligadata.Exceptions.{InvalidArgumentException, FatalAdapterException, StackTrace}
import com.ligadata.KamanjaBase.DataDelimiters

object IbmMqConsumer extends InputAdapterObj {
  def CreateInputAdapter(inputConfig: AdapterConfiguration, callerCtxt: InputAdapterCallerContext, execCtxtObj: ExecContextObj, cntrAdapter: CountersAdapter): InputAdapter = new IbmMqConsumer(inputConfig, callerCtxt, execCtxtObj, cntrAdapter)
}

class IbmMqConsumer(val inputConfig: AdapterConfiguration, val callerCtxt: InputAdapterCallerContext, val execCtxtObj: ExecContextObj, cntrAdapter: CountersAdapter) extends InputAdapter {
  private def printFailure(ex: Exception) {
    if (ex != null) {
      if (ex.isInstanceOf[JMSException]) {
        processJMSException(ex.asInstanceOf[JMSException])
      } else {
        LOG.error(ex)
      }
    }
  }

  private def processJMSException(jmsex: JMSException) {
    LOG.error("MQ input adapter " + qc.Name + ": JMSException summary",jmsex)
    var innerException: Throwable = jmsex.getLinkedException
    if (innerException != null) {
      LOG.error("MQ input adapter " + qc.Name + ":Inner exception(s):")
    }
    while (innerException != null) {
      LOG.error(innerException)
      innerException = innerException.getCause
    }
  }

  private[this] val LOG = LogManager.getLogger(getClass);

  //BUGBUG:: Not Checking whether inputConfig is really QueueAdapterConfiguration or not. 
  private[this] val qc = IbmMqAdapterConfiguration.GetAdapterConfig(inputConfig)
  private[this] val lock = new Object()
  private[this] val kvs = scala.collection.mutable.Map[String, (IbmMqPartitionUniqueRecordKey, IbmMqPartitionUniqueRecordValue, IbmMqPartitionUniqueRecordValue)]()

  var connection: Connection = null
  var session: Session = null
  var destination: Destination = null
  var consumer: MessageConsumer = null

  var executor: ExecutorService = _
  val input = this

  private val max_retries = 3
  private val max_sleep_time = 60000
  private var retry_counter = 0
  private val initSleepTimer = 2000
  private var currSleepTime = initSleepTimer
  private var isShutdown = false


  override def Shutdown: Unit = lock.synchronized {
    isShutdown = true
    StopProcessing
  }

  override def StopProcessing: Unit = lock.synchronized {
    LOG.debug("MQ input adapter " + qc.Name + ":===============> Called StopProcessing")
    retry_counter = 0

    //BUGBUG:: Make sure we finish processing the current running messages.
    while (consumer != null) {
      try {
        consumer.close()
        consumer = null
        retry_counter = 0
      } catch {
        case jmsex: Exception => {
          LOG.error("MQ input adapter " + qc.Name + ": Consumer could not be closed.")
          checkForRetry(jmsex)
        }
      }
    }

    // Do we need to close destination ??
    while (session != null) {
      try {
        session.close()
        session = null
        retry_counter = 0
      } catch {
        case jmsex: Exception => {
          LOG.error("MQ input adapter " + qc.Name + ": Session could not be closed.")
          checkForRetry(jmsex)
        }
      }
    }

    while (connection != null) {
      try {
        connection.close()
        connection = null
        retry_counter = 0
      } catch {
        case jmsex: Exception => {
          LOG.error("MQ input adapter " + qc.Name + ": Connection could not be closed.")
          checkForRetry(jmsex)
        }
      }
    }
    retry_counter = 0

    if (executor != null) {
      executor.shutdownNow()
      while (executor.isTerminated == false) {
        Thread.sleep(100) // sleep 100ms and then check
      }
    }

    destination = null
    executor = null
  }

  // Each value in partitionInfo is (PartitionUniqueRecordKey, PartitionUniqueRecordValue, Long, PartitionUniqueRecordValue) key, processed value, Start transactionid, Ignore Output Till given Value (Which is written into Output Adapter) 
  override def StartProcessing(partitionInfo: Array[StartProcPartInfo], ignoreFirstMsg: Boolean): Unit = lock.synchronized {
    var receivedMessage: Message = null
    isShutdown = false

    LOG.debug("MQ input adapter " + qc.Name + ": ===============> Called StartProcessing")
    if (partitionInfo == null || partitionInfo.size == 0)
      return

    val partInfo = partitionInfo.map(quad => { (quad._key.asInstanceOf[IbmMqPartitionUniqueRecordKey], quad._val.asInstanceOf[IbmMqPartitionUniqueRecordValue], quad._validateInfoVal.asInstanceOf[IbmMqPartitionUniqueRecordValue]) })

    try {
      val ff = JmsFactoryFactory.getInstance(JmsConstants.WMQ_PROVIDER)
      val cf = ff.createConnectionFactory()

      cf.setStringProperty(CommonConstants.WMQ_HOST_NAME, qc.host_name)
      cf.setIntProperty(CommonConstants.WMQ_PORT, qc.port)
      cf.setStringProperty(CommonConstants.WMQ_CHANNEL, qc.channel)
      cf.setIntProperty(CommonConstants.WMQ_CONNECTION_MODE, qc.connection_mode)
      cf.setStringProperty(CommonConstants.WMQ_QUEUE_MANAGER, qc.queue_manager)
      if (qc.ssl_cipher_suite.size > 0)
        cf.setStringProperty(CommonConstants.WMQ_SSL_CIPHER_SUITE, qc.ssl_cipher_suite)

      retry_counter = 0
      // If we cant create a connection, return immediately.. invalid parameters
      while (connection == null) {
        try {
          connection = cf.createConnection()
          retry_counter = 0
        } catch {
          case jmsex: Exception => {
            LOG.error("MQ input adapter " + qc.Name + ": Connection could not be created")
            checkForRetry(jmsex)
          }
        }
      }

      while (session == null) {
        try {
          session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
          retry_counter = 0
        } catch {
          case jmsex: Exception => {
            LOG.error("MQ input adapter " + qc.Name + ": Session could not be created")
            checkForRetry(jmsex)
          }
        }
      }

      if (qc.queue_name != null && qc.queue_name.size > 0)
        destination = createQueue(qc.queue_name)
      else if (qc.topic_name != null && qc.topic_name.size > 0)
        destination = createTopic(qc.queue_name)
      else {
        throw new FatalAdapterException("MQ input adapter " + qc.Name + ": Both Queue and Topic names are missing ", new InvalidArgumentException("Missing queue anme and topic name"))
      }

      while (consumer == null) {
        try {
           consumer = session.createConsumer(destination)
          retry_counter = 0
        } catch {
          case jmsex: JMSException => {
            LOG.error("MQ input adapter " + qc.Name + ": Unable to create JMX Consumer ")
            checkForRetry(jmsex)
          }
        }
      }

      var isStarted = false
      while (!isStarted) {
        try {
          connection.start()
          isStarted = true
          retry_counter = 0
        } catch {
          case jmsex: JMSException => {
            LOG.error("MQ input adapter " + qc.Name + ": Unable to start JMX Connection ")
            checkForRetry(jmsex)
          }
        }
      }
    } catch {
      case fae: FatalAdapterException => throw fae
      case jmsex: Exception =>
        LOG.error("MQ input adapter " + qc.Name + ": Exception establishing a connection to the adapter.")
        printFailure(jmsex)
        throw new FatalAdapterException("Fata Adapter Exception", jmsex)
    }

    val delimiters = new DataDelimiters()
    delimiters.keyAndValueDelimiter = qc.keyAndValueDelimiter
    delimiters.fieldDelimiter = qc.fieldDelimiter
    delimiters.valueDelimiter = qc.valueDelimiter

    var threads: Int = 1
    if (threads == 0)
      threads = 1

    // create the consumer streams
    executor = Executors.newFixedThreadPool(threads)

    kvs.clear

    partInfo.foreach(quad => {
      kvs(quad._1.Name) = quad
    })

    LOG.debug("MQ input adapter " + qc.Name + ": KV Map =>")
    kvs.foreach(kv => {
      LOG.debug("MQ input adapter " + qc.Name + ": Key:%s => Val:%s".format(kv._2._1.Serialize, kv._2._2.Serialize))
    })

    try {
      for (i <- 0 until threads) {
        executor.execute(new Runnable() {
          override def run() {
            var execThread: ExecContext = null
            var cntr: Long = 0
            var checkForPartition = true
            val uniqueKey = new IbmMqPartitionUniqueRecordKey
            val uniqueVal = new IbmMqPartitionUniqueRecordValue

            uniqueKey.Name = qc.Name
            uniqueKey.QueueManagerName = qc.queue_manager
            uniqueKey.ChannelName = qc.channel
            uniqueKey.QueueName = qc.queue_name
            uniqueKey.TopicName = qc.topic_name

            try {
              breakable {
                while (!isShutdown ) {
                  var executeCurMsg = true

                  try {
                    receivedMessage = null
                    // Keep retrying to get the message.  Eventually this should be redesigned, that the engine should
                    // be able to be notified of this issue...  ALSO:  Do we need to rebuild all the MQ sturctures?
                    while (receivedMessage == null && !isShutdown) {
                      try {
                        receivedMessage = consumer.receive()
                        currSleepTime = initSleepTimer
                      } catch {
                        case jmsex: JMSException => {
                          LOG.error("MQ input adapter " + qc.Name + ":Exception receiving message from MQ Consumer.")
                          printFailure(jmsex)
                          var blah = getSleepTime
                           Thread.sleep(blah)
                        }
                      }
                    }

                    val readTmNs = System.nanoTime
                    val readTmMs = System.currentTimeMillis

                    executeCurMsg = (receivedMessage != null)

                    if (checkForPartition) {
                      checkForPartition = false
                      execThread = execCtxtObj.CreateExecContext(input, uniqueKey, callerCtxt)
                    }

                    if (executeCurMsg) {
                      try {

                        var msgData: Array[Byte] = null
                        var msgId: String = null

                        if (receivedMessage.isInstanceOf[BytesMessage]) {
                          val bytmsg = receivedMessage.asInstanceOf[BytesMessage]
                          val tmpmsg = new Array[Byte](bytmsg.getBodyLength().toInt)
                          bytmsg.readBytes(tmpmsg)
                          msgData = tmpmsg
                          msgId = bytmsg.getJMSMessageID()
                        } else if (receivedMessage.isInstanceOf[TextMessage]) {
                          val strmsg = receivedMessage.asInstanceOf[TextMessage]
                          msgData = strmsg.getText.getBytes
                          msgId = strmsg.getJMSMessageID()
                        }

                        if (msgData != null) {
                          uniqueVal.MessageId = msgId
                          execThread.execute(msgData, qc.formatOrInputAdapterName, uniqueKey, uniqueVal, readTmNs, readTmMs, false, qc.associatedMsg, delimiters)
                          // consumerConnector.commitOffsets // BUGBUG:: Bad way of calling to save all offsets
                          cntr += 1
                          val key = Category + "/" + qc.Name + "/evtCnt"
                          cntrAdapter.addCntr(key, 1) // for now adding each row

                        } else {
                          LOG.error("MQ input adapter " + qc.Name + ": Found unhandled message :" + receivedMessage.getClass().getName())
                        }
                      } catch {
                        case e: Exception => {
                          LOG.error("MQ input adapter " + qc.Name + ": Failed with Message:" + e.getMessage)
                          printFailure(e)
                        }
                      }
                    } else {
                    }
                  } catch {
                    case e: Exception => {
                      LOG.error("MQ input adapter " + qc.Name + ": Failed with Message:" + e.getMessage)
                      printFailure(e)
                    }
                  }
                  if (executor.isShutdown) {
                    LOG.info("MQ input adapter " + qc.Name + ": Adapter is stopping processing incoming messages.  Shutdown detected")
                    break
                  }
                }
              }
            } catch {
              case e: Exception => {
                LOG.error("MQ input adapter " + qc.Name + ": Failed with Reason:%s Message:%s".format(e.getCause, e.getMessage))
                printFailure(e)
              }
            }
            LOG.debug("MQ input adapter " + qc.Name + ": ===========================> Exiting Thread")
          }
        });
      }
    } catch {
      case e: Exception => {
        LOG.error("MQ input adapter " + qc.Name + ": Failed to setup Streams. Reason:%s Message:%s".format(e.getCause, e.getMessage))
        printFailure(e)
      }
    }
  }

  private def GetAllPartitionsUniqueKeys: Array[PartitionUniqueRecordKey] = lock.synchronized {
    val uniqueKey = new IbmMqPartitionUniqueRecordKey

    uniqueKey.Name = qc.Name
    uniqueKey.QueueManagerName = qc.queue_manager
    uniqueKey.ChannelName = qc.channel
    uniqueKey.QueueName = qc.queue_name
    uniqueKey.TopicName = qc.topic_name

    Array[PartitionUniqueRecordKey](uniqueKey)
  }

  override def GetAllPartitionUniqueRecordKey: Array[PartitionUniqueRecordKey] = lock.synchronized {
    GetAllPartitionsUniqueKeys
  }

  override def DeserializeKey(k: String): PartitionUniqueRecordKey = {
    val key = new IbmMqPartitionUniqueRecordKey
    try {
      LOG.debug("MQ input adapter " + qc.Name + ": Deserializing Key:" + k)
      key.Deserialize(k)
    } catch {
      case e: Exception => {
        LOG.error("MQ input adapter " + qc.Name + ": Failed to deserialize Key:%s. Reason:%s Message:%s".format(k, e.getCause, e.getMessage))
        printFailure(e)
        throw new FatalAdapterException("Invalid Key " + k, e)
      }
    }
    key
  }

  override def DeserializeValue(v: String): PartitionUniqueRecordValue = {
    val vl = new IbmMqPartitionUniqueRecordValue
    if (v != null) {
      try {
        LOG.debug("MQ input adapter " + qc.Name + ": Deserializing Value:" + v)
        vl.Deserialize(v)
      } catch {
        case e: Exception => {
          LOG.error("MQ input adapter " + qc.Name + ": Failed to deserialize Value:%s. Reason:%s Message:%s".format(v, e.getCause, e.getMessage))
          printFailure(e)
          throw FatalAdapterException("Invalid Value " + v, e)
        }
      }
    }
    vl
  }

  private def createQueue(tName: String): Destination = {
    var tDest: Destination = null
    while (tDest == null) {
      try {
        tDest = session.createQueue(tName)
        retry_counter = 0
      } catch {
        case jmsex: JMSException => {
          LOG.error("MQ input adapter " + qc.Name + ": Destination could not be created. ")
          checkForRetry(jmsex)
        }
      }
    }
    tDest
  }

  private def createTopic(tName: String): Destination = {
    var tDest: Destination = null
    while (tDest == null) {
      try {
        tDest = session.createTopic(tName)
        retry_counter = 0
      } catch {
        case jmsex: JMSException => {
          LOG.error("MQ input adapter " + qc.Name + ": Destination could not be created ")
          checkForRetry(jmsex)
        }
      }
    }
    tDest
  }

  private def checkForRetry (jmsex: Exception): Unit = {
    retry_counter += 1
    LOG.error("MQ input adapter " + qc.Name + ": Retrying "+retry_counter + "/"+max_retries)
    printFailure(jmsex)
    if (retry_counter >= max_retries) {
      throw new FatalAdapterException("MQ input adapter " + qc.Name + ": Fatal exception",jmsex)
    }
    Thread.sleep(initSleepTimer)
  }

  private def getSleepTime: Int = {
    val tts = scala.math.min(currSleepTime, max_sleep_time)
    currSleepTime = tts * 2
    tts
  }

  // Not yet implemented
  override def getAllPartitionBeginValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = {
    return Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]()
  }

  // Not yet implemented
  override def getAllPartitionEndValues: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = {
    return Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)]()
  }

}

