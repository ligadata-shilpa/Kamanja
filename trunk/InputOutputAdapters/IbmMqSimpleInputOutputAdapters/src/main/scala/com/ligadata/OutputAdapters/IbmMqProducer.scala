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

import java.util.Properties
//import java.util.logging.LogManager
import org.apache.logging.log4j.{ Logger, LogManager }
import com.ligadata.InputOutputAdapterInfo.{ AdapterConfiguration, OutputAdapter, OutputAdapterObj, CountersAdapter }
import com.ligadata.AdaptersConfiguration.IbmMqAdapterConfiguration
import javax.jms.{ Connection, Destination, JMSException, Message, MessageProducer, Session, TextMessage, BytesMessage }
import com.ibm.msg.client.jms.JmsConnectionFactory
import com.ibm.msg.client.jms.JmsFactoryFactory
import com.ibm.msg.client.wmq.WMQConstants
import com.ibm.msg.client.wmq.common.CommonConstants
import com.ibm.msg.client.jms.JmsConstants
import com.ligadata.Exceptions.{InvalidArgumentException, FatalAdapterException, StackTrace}

object IbmMqProducer extends OutputAdapterObj {
  def CreateOutputAdapter(inputConfig: AdapterConfiguration, cntrAdapter: CountersAdapter): OutputAdapter = new IbmMqProducer(inputConfig, cntrAdapter)
}

class IbmMqProducer(val inputConfig: AdapterConfiguration, cntrAdapter: CountersAdapter) extends OutputAdapter {
  private[this] val LOG = LogManager.getLogger(getClass);
  private val max_retries = 3
  private val max_sleep_time = 60000
  private var retry_counter = 0
  private val initSleepTimer = 2000
  private var currSleepTime = initSleepTimer

  //BUGBUG:: Not Checking whether inputConfig is really QueueAdapterConfiguration or not. 
  private[this] val qc = IbmMqAdapterConfiguration.GetAdapterConfig(inputConfig)
  private def printFailure(ex: Exception) {
    if (ex != null) {
      if (ex.isInstanceOf[JMSException]) {
        processJMSException(ex.asInstanceOf[JMSException])
      } else {
        LOG.error("MQ input adapter " + qc.Name, ex)
      }
    }
  }

  private def processJMSException(jmsex: JMSException) {
    LOG.error("MQ input adapter " + qc.Name, jmsex)
    var innerException: Throwable = jmsex.getLinkedException
    if (innerException != null) {
      LOG.error("MQ input adapter " + qc.Name + "Inner exception(s):")
    }
    while (innerException != null) {
      LOG.error("MQ input adapter " + qc.Name,innerException)
      innerException = innerException.getCause
    }
  }

  private var connection: Connection = null
  private var session: Session = null
  private var destination: Destination = null
  private var producer: MessageProducer = null
  private var isStartedSuccessfully = false

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

    while (connection == null) {
      try {
        connection = cf.createConnection()
        retry_counter = 0
      } catch {
        case jmsex: JMSException => {
          LOG.error("MQ input adapter " + qc.Name + ": connection could not be created. ")
          checkForRetry(jmsex)
        }
      }
    }

    while (session == null) {
      try {
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
        retry_counter = 0
      } catch {
        case jmsex: JMSException => {
          LOG.error("MQ input adapter " + qc.Name + ": session could not be created. ")
          checkForRetry(jmsex)
        }
      }
    }


    if (qc.queue_name != null && qc.queue_name.size > 0)
      destination = createQueue(qc.queue_name)
    else if (qc.topic_name != null && qc.topic_name.size > 0)
      destination = createTopic(qc.topic_name)
    else {
      throw new FatalAdapterException("MQ input adapter " + qc.Name + ": Both Queue and Topic names are missing ", new InvalidArgumentException("Missing queue anme and topic name"))
    }

    while (producer == null) {
      try {
        producer = session.createProducer(destination)
        retry_counter = 0
      } catch {
        case jmsex: JMSException => {
          LOG.error("MQ input adapter " + qc.Name + ": producer could not be created. ")
          checkForRetry(jmsex)
        }
      }
    }

    while (!isStartedSuccessfully) {
      try {
        connection.start()
        isStartedSuccessfully = true
        retry_counter = 0
      } catch {
        case jmsex: JMSException => {
          LOG.error("MQ input adapter " + qc.Name + ": producer could not be started. ")
          checkForRetry(jmsex)
        }
      }
    }
  } catch {
    case e : FatalAdapterException => throw e
    case jmsex: Exception => {
      LOG.error("MQ output adapter " + qc.Name + ": Exception establishing a connection to the adapter.")
      printFailure(jmsex)
      throw new FatalAdapterException("Fata Adapter Exception", jmsex)
    }
  }

  // To send an array of messages. messages.size should be same as partKeys.size
  override def send(messages: Array[Array[Byte]], partKeys: Array[Array[Byte]]): Unit = {
    var isSendSuccessful: Boolean = false

    if (messages.size != partKeys.size) {
      LOG.error("Message and Partition Keys hould has same number of elements. Message has %d and Partition Keys has %d".format(messages.size, partKeys.size))
      return
    }
    if (messages.size == 0) return

    try {
      // Op is not atomic
      messages.foreach(message => {
        isSendSuccessful = false
        // Do we need text Message or Bytes Message?
        if (qc.msgType == com.ligadata.AdaptersConfiguration.MessageType.fByteArray) {
          val outmessage = session.createBytesMessage()
          outmessage.writeBytes(message)
          outmessage.setStringProperty("ContentType", qc.content_type)
          sendMessage(outmessage)
        } else { // By default we are taking (qc.msgType == com.ligadata.AdaptersConfiguration.MessageType.fText)
          val outmessage = session.createTextMessage(new String(message))
          outmessage.setStringProperty("ContentType", qc.content_type)
          sendMessage(outmessage)
        }
        val key = Category + "/" + qc.Name + "/evtCnt"
        cntrAdapter.addCntr(key, 1) // for now adding each row
      })
    } catch {
      case e: FatalAdapterException => throw e
      case jmsex: Exception => {
        LOG.error("MQ output adapter " + qc.Name + ": Exception establishing a connection to the adapter.")
        printFailure(jmsex)
        throw new FatalAdapterException("Fata Adapter Exception", jmsex)
      }
    }
  }

  override def Shutdown(): Unit = {
    if (producer != null) {
      while (producer != null) {
        try {
          producer.close()
          producer = null
        } catch {
          case jmsex: Exception => {
            LOG.error("MQ output adapter " + qc.Name + ": Producer could not be closed.")
            checkForRetry(jmsex)
          }
        }
      }
    }

    // Do we need to close destination ??
    if (session != null) {
      while (session != null) {
        try {
          session.close()
          session = null
        } catch {
          case jmsex: Exception => {
            LOG.error("MQ output adapter " + qc.Name + ": Session could not be closed.")
            checkForRetry(jmsex)
          }
        }
      }
    }
    if (connection != null) {
      while (connection != null) {
        try {
          connection.close()
          connection = null
        } catch {
          case jmsex: Exception => {
            LOG.error("MQ output adapter " + qc.Name + ": Connection could not be closed.")
            checkForRetry(jmsex)
          }
        }
      }
    }
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

  private def sendMessage (outmessage: Message) = {
    var isSendSuccessful: Boolean = false
    while (!isSendSuccessful) {
      try {
        producer.send(outmessage)
        isSendSuccessful = true
      } catch {
        case jmsex: Exception => {
          LOG.error("MQ input adapter " + qc.Name + ": Session could not be created")
          checkForRetry(jmsex)
        }
      }
    }
  }
}

