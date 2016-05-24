package com.ligadata

import com.ligadata.metadataapiservice.APIService
import com.ligadata.KamanjaManager._
import org.apache.logging.log4j.LogManager

import scala.actors.threadpool.{TimeUnit, Executors}

/**
  * Created by Yasser on 5/24/2016.
  */
object KamanjaService {

  private val LOG = LogManager.getLogger(getClass)
  private val executor = Executors.newFixedThreadPool(3)

  def main(args : Array[String]) : Unit = {

    scala.sys.addShutdownHook({
      if (!KamanjaConfiguration.shutdown) {
        LOG.warn("KAMANJA-Service: Received shutdown request")
        KamanjaConfiguration.shutdown = true // Setting the global shutdown

        shutdown()
      }
    })


    val kamanjaEngineThread = new Runnable() {
      override def run(): Unit = {
        LOG.warn("starting KAMANJA-MANAGER")

        //TODO : pass engine config file path (hardcoded temporarily)
        //TODO : wait until cluster config is uploaded

        KamanjaManager.KamanjaManager.startKamanjaManager(args)
      }
    }
    executor.execute(kamanjaEngineThread)

    //run metadata api


    //run rest api
    val apiServiceThread = new Runnable() {
      override def run(): Unit = {
        LOG.warn("starting APIService")

        //TODO : pass config file path (hardcoded temporarily)

        APIService.startAPISevrice(args)
      }
    }
    executor.execute(apiServiceThread)
  }

  def shutdown(): Unit ={

    APIService.shutdownAPISevrice()

    executor.shutdown()
    try {
      if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
        executor.shutdownNow()
        if (!executor.awaitTermination(1, TimeUnit.SECONDS))
          LOG.warn("Pool did not terminate ")
      }
    } catch  {
      case ie : InterruptedException =>
        LOG.warn("InterruptedException", ie)
        executor.shutdownNow()
        Thread.currentThread().interrupt()

      case th : Throwable =>
        LOG.warn("Throwable", th)

    }
  }
}
