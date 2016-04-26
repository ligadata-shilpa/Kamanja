package com.ligadata.InputAdapters

import java.io.IOException
import com.ligadata.AdaptersConfiguration.SmartFileAdapterConfiguration
import org.apache.logging.log4j.LogManager

import scala.actors.threadpool.Executors

/**
  *
  *
  * @param adapterConfig
  * @param fileHandler file to read messages from
  * @param startOffset offset in the file to start with
  * @param consumerContext has required params
  * @param messageFoundCallback to call for every read message
  * @param finishCallback call when finished reading
  */
class FileMessageExtractor(adapterConfig : SmartFileAdapterConfiguration,
                           fileHandler: SmartFileHandler,
                           startOffset : Int,
                           consumerContext : SmartFileConsumerContext,
                           messageFoundCallback : (SmartFileMessage, SmartFileConsumerContext) => Unit,
                           finishCallback : (SmartFileHandler, SmartFileConsumerContext, Int) => Unit ) {

  private val maxlen: Int = adapterConfig.monitoringConfig.workerBufferSize * 1024 * 1024 //in MB
  private var message_separator : Char = adapterConfig.monitoringConfig.messageSeparator

  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)
  private var currentMsgNum = -1 // to start from zero
  private var globalOffset = 0

  private val extractExecutor = Executors.newFixedThreadPool(1)
  private val updatExecutor = Executors.newFixedThreadPool(1)

  private val StatusUpdateInterval = 1000//ms

  private var finished = false



  def extractMessages() : Unit = {

    if(!fileHandler.exists()){
      finishCallback(fileHandler, consumerContext, SmartFileConsumer.FILE_STATUS_NOT_FOUND)
    }

    else {
      //just run it in a separate thread
      val extractorThread = new Runnable() {
        override def run(): Unit = {
          readBytesChunksFromFile()
        }
      }
      extractExecutor.execute(extractorThread)

      //keep updating status so leader knows participant is working fine
      val statusUpdateThread = new Runnable() {
        override def run(): Unit = {
          try {
            while (!finished) {
              //put filename~offset~timestamp
              val data = fileHandler.getFullPath + "~" + currentMsgNum + "~" + System.nanoTime
              logger.debug("SMART FILE CONSUMER - Node {} with partition {} is updating status to value {}",
                consumerContext.nodeId, consumerContext.partitionId.toString, data)
              consumerContext.envContext.saveConfigInClusterCache(consumerContext.statusUpdateCacheKey, data.getBytes)


              Thread.sleep(StatusUpdateInterval)

            }
          }
          catch {
            case ie: InterruptedException => {}
            case e: Exception => logger.error("", e)
          }
        }
      }
      updatExecutor.execute(statusUpdateThread)
    }
  }

  private def readBytesChunksFromFile(): Unit = {

    val byteBuffer = new Array[Byte](maxlen)

    var readlen = 0
    var len: Int = 0
    var totalLen = 0
    var chunkNumber = 0

    val fileName = fileHandler.getFullPath

    logger.info("Smart File Consumer - Starting reading messages from file " + fileName)

    //val offset = file.offset
    //val partMap = file.partMap
    //val fileHandler = file.fileHandler

    // Grab the InputStream from the file and start processing it.  Enqueue the chunks onto the BufferQ for the
    // worker bees to pick them up.

    try {
      fileHandler.openForRead()
    } catch {
      // Ok, sooo if the file is not Found, either someone moved the file manually, or this specific destination is not reachable..
      // We just drop the file, if it is still in the directory, then it will get picked up and reprocessed the next tick.
      case fio: java.io.FileNotFoundException => {
        logger.error("SMART_FILE_CONSUMER Exception accessing the file for processing the file - File is missing",fio)
        //markFileProcessingEnd(fileName)
        //fileCacheRemove(fileName)
        finishCallback(fileHandler, consumerContext, SmartFileConsumer.FILE_STATUS_NOT_FOUND)
        shutdownThreads
        return
      }
      case fio: IOException => {
        logger.error("SMART_FILE_CONSUMER Exception accessing the file for processing ",fio)
        finishCallback(fileHandler, consumerContext, SmartFileConsumer.FILE_STATUS_CORRUPT)
        shutdownThreads
        return
      }
      case ex : Exception => {
        logger.error("", ex)
        finishCallback(fileHandler, consumerContext, SmartFileConsumer.FILE_STATUS_CORRUPT)
        shutdownThreads

        return
      }
    }

    var waitedCntr = 0
    var tempFailure = 0

    try {
      var previousLeftOverBytes: Array[Byte] = Array()
      do {
        /*waitedCntr = 0
      val st = System.currentTimeMillis
      while ((BufferCounters.inMemoryBuffersCntr.get * 2 + partitionSelectionNumber + 2) * maxlen > maxBufAllowed) { // One counter for bufferQ and one for msgQ and also taken concurrentKafkaJobsRunning and 2 extra in memory
        if (waitedCntr == 0) {
          logger.warn("SMART FILE ADDAPTER (" + partitionId + ") : exceed the allowed memory size (%d) with %d buffers. Halting for free slot".format(maxBufAllowed,
            BufferCounters.inMemoryBuffersCntr.get * 2))
        }
        waitedCntr += 1
        Thread.sleep(throttleTime)
      }

      if (waitedCntr > 0) {
        val timeDiff = System.currentTimeMillis - st
        logger.warn("%d:Got slot after waiting %dms".format(partitionId, timeDiff))
      }*/

        BufferCounters.inMemoryBuffersCntr.incrementAndGet() // Incrementing when we enQBuffer and Decrementing when we deQMsg
        var isLastChunk = false
        try {
          readlen = fileHandler.read(byteBuffer, maxlen - 1)
          // if (readlen < (maxlen - 1)) isLastChunk = true
        } catch {
          /*case ze: ZipException => {
          logger.error("Failed to read file, file currupted " + fileName, ze)
          //val buffer = MonitorUtils.toCharArray(byteBuffer)
          val GenericBufferToChunk = new BufferToChunk(readlen, byteBuffer.slice(0, readlen), chunkNumber, fileHandler, FileProcessor.CORRUPT_FILE, isLastChunk, partMap)
          enQBuffer(GenericBufferToChunk)
          return
        }*/
          case ioe: IOException => {
            logger.error("Failed to read file " + fileName, ioe)
            /*val buffer = MonitorUtils.toCharArray(byteBuffer)
          val GenericBufferToChunk = new BufferToChunk(readlen, byteBuffer.slice(0, readlen), chunkNumber, fileHandler, FileProcessor.BROKEN_FILE, isLastChunk, partMap)
          enQBuffer(GenericBufferToChunk)*/
            finishCallback(fileHandler, consumerContext, SmartFileConsumer.FILE_STATUS_CORRUPT)
            shutdownThreads
            return
          }
          case e: Exception => {
            logger.error("Failed to read file, file corrupted " + fileName, e)
            /*val buffer = MonitorUtils.toCharArray(byteBuffer)
          val GenericBufferToChunk = new BufferToChunk(readlen, byteBuffer.slice(0, readlen), chunkNumber, fileHandler, FileProcessor.CORRUPT_FILE, isLastChunk, partMap)
          enQBuffer(GenericBufferToChunk)*/
            finishCallback(fileHandler, consumerContext, SmartFileConsumer.FILE_STATUS_CORRUPT)
            shutdownThreads
            return
          }
        }
        if (readlen > 0) {
          totalLen += readlen
          len += readlen

          //val GenericBufferToChunk = new BufferToChunk(readlen, byteBuffer.slice(0, readlen), chunkNumber, fileHandler, offset, isLastChunk, partMap)
          val bytesToExtract = if (previousLeftOverBytes.length == 0) byteBuffer.slice(0, readlen) else previousLeftOverBytes ++ byteBuffer.slice(0, readlen)
          val result = extractMessages(bytesToExtract)
          previousLeftOverBytes = result

        }

      } while (readlen > 0)

      if (previousLeftOverBytes.length > 0) {
        val lastMsg = previousLeftOverBytes
        if (lastMsg.length == 1 && lastMsg(0) == message_separator) {
          //only separator is left
        }
        else {
          currentMsgNum += 1
          //println(s"*************** last message ($msgNum): " + new String(lastMsg))
          if (currentMsgNum >= startOffset) {
            val msgOffset = currentMsgNum // offset of the message in the file
            val smartFileMessage = new SmartFileMessage(lastMsg, msgOffset, false, false, fileHandler, null, msgOffset)
            messageFoundCallback(smartFileMessage, consumerContext)
          }

          //a message is extracted to passed to engine, update offset in cache
          globalOffset = globalOffset + lastMsg.length
          //consumerContext.envContext.saveConfigInClusterCache(consumerContext.fileOffsetCacheKey, globalOffset.toString.getBytes)

        }
      }
    }
    catch {
      case ioe: IOException => {
        logger.error("SMART FILE CONSUMER: Exception while accessing the file for processing " + fileName, ioe)
        finishCallback(fileHandler, consumerContext, SmartFileConsumer.FILE_STATUS_CORRUPT)
        shutdownThreads
        return
      }
    }

    // Done with this file... mark is as closed
    try {
      // markFileAsFinished(fileName)
      if (fileHandler != null) fileHandler.close
      //bis = null

    } catch {
      case ioe: IOException => {
        logger.error("SMART FILE CONSUMER: Exception while closing file " + fileName, ioe)
      }
    }
    finally{
      if(finishCallback != null)
        finishCallback(fileHandler, consumerContext, SmartFileConsumer.FILE_STATUS_FINISHED)
      shutdownThreads()
    }

  }

  private def shutdownThreads(): Unit ={
    finished = true
    logger.debug("File message Extractor - shutting down updatExecutor")
    MonitorUtils.shutdownAndAwaitTermination(updatExecutor, "file message extracting status updator")

    logger.debug("File message Extractor - shutting down extractExecutor")
    MonitorUtils.shutdownAndAwaitTermination(extractExecutor, "file message extractor")
  }

  private def extractMessages(chunk : Array[Byte]) : Array[Byte] = {
    var indx = 0
    var prevIndx = indx


    chunk.foreach(x => {
      if (x.asInstanceOf[Char] == message_separator) {
        val newMsg: Array[Byte] = chunk.slice(prevIndx, indx)
        if(newMsg.length > 0) {
          currentMsgNum += 1
          //println(s"*************** new message ($msgNum): " + new String(newMsg))
          if(currentMsgNum >= startOffset) {//send messages that are only after startOffset
            val msgOffset = currentMsgNum // offset of the message in the file
            val smartFileMessage = new SmartFileMessage(newMsg, msgOffset, false, false, fileHandler, null, msgOffset)
            messageFoundCallback(smartFileMessage, consumerContext)

            //a message is extracted to passed to engine, update offset in cache
            //consumerContext.envContext.saveConfigInClusterCache(consumerContext.fileOffsetCacheKey, globalOffset.toString.getBytes)
          }
          prevIndx = indx + 1
          globalOffset = globalOffset + newMsg.length + 1 //(1 for separator length)
        }
      }
      indx = indx + 1
    })

    if(prevIndx == chunk.length)
      Array()
    else
      chunk.slice(prevIndx, chunk.length)
  }
}
