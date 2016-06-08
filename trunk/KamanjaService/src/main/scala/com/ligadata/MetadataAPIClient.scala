package com.ligadata

import java.io.{IOException, InputStreamReader, PrintWriter, BufferedReader}
import java.net.{UnknownHostException, Socket}

import org.apache.logging.log4j.LogManager

/**
  * Created by Yasser on 6/7/2016.
  */
object MetadataAPIClient {

  private val logger = LogManager.getLogger(getClass)

  def main(args : Array[String]): Unit ={

    //TODO : pas params as arguments for now, better use a cofnig file
    val hostName = args(0)
    val portNumber = args(1).toInt

    if(isKamanjaLocal){
      logger.info("Running Metadata API Client on local machine")
      connectToLocal(hostName, portNumber)
    }
    else{
      logger.info("Running Metadata API Client remotely")
      connectToRemote(hostName, portNumber)
    }

  }

  def isKamanjaLocal: Boolean ={
    true;//TODO : implement this
  }

  /**
    * connect using rest api
    * @param hostName
    * @param portNumber
    */
  def connectToRemote(hostName : String, portNumber : Int) : Unit = ???

  /**
    * connect using socket connection
    *
    * @param hostName
    * @param portNumber
    */
  def connectToLocal(hostName : String, portNumber : Int): Unit ={
    try {
      val socket = new Socket(hostName, portNumber)
      val out : PrintWriter = new PrintWriter(socket.getOutputStream, true)
      val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
      val stdIn = new BufferedReader(new InputStreamReader(System.in))

      var userInput = ""
      print("kamanja>")
      userInput = stdIn.readLine
      while ( userInput != null) {
        logger.info("MetadataAPIClient - got command {}", userInput)
        out.println(userInput)
        print("kamanja>")

        userInput = stdIn.readLine
      }
    } catch {
      case e : UnknownHostException =>
        logger.error("Don't know about host " + hostName, e)
        System.exit(1)
      case e: IOException =>
        logger.error("Couldn't get I/O for the connection to " + hostName, e)
        System.exit(1)

      case e: Throwable =>
        logger.error("Throwable: ", e)
        System.exit(1)
    }
  }
}
