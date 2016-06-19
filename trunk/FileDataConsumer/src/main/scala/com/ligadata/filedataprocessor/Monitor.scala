package com.ligadata.filedataprocessor

/**
  * Created by dan on 1/9/15.
  */
object Monitor {
  def main(args: Array[String]): Unit = {
    var config = args(0)
    var properties = scala.collection.mutable.Map[String,String]()
    val lines = scala.io.Source.fromFile(config).getLines.toList
    lines.foreach(line => {
      //Handle empty lines also
      if (!line.isEmpty() && !line.startsWith("#")) {
        val lProp = line.split("=")
        try {
          properties(lProp(0)) = lProp(1)
        } catch {
          case iobe: IndexOutOfBoundsException => {
            return
          }
          case e: Throwable => {
            return
          }
        }
      }
    })

    val monitor: FileMonitor = new FileMonitor(properties, args)
    val monitorThread = new Thread(monitor)
    monitorThread.start()

  }


import java.io.File
import java.lang.ProcessBuilder.Redirect
import java.net.URLEncoder
import com.ligadata.ZooKeeper.CreateClient
import com.ligadata.ZooKeeper.CreateClient
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException.NoNodeException


case class fileStats(fileName: String, offset: Long, lastSeen: Long)

class FileMonitor(val props: scala.collection.mutable.Map[String,String], oargs: Array[String]) extends Runnable {
  lazy val loggerName = this.getClass.getName

  private var zkcConnectString: String = _
  private var znodePath: String = _
  private var zkClient: CuratorFramework = null
  private var maxTimeAllowedToBeStuck = props.getOrElse("monitoringInterval",300000).asInstanceOf[Int]
  private var numberOfConsumers = props.getOrElse("numberOfConsumers",2).asInstanceOf[Int]
  private var refreshTime = props.getOrElse("monitoringInterval",10000).asInstanceOf[Int]
  private var isTestMode: Boolean  = props.getOrElse("isTestMode",true).asInstanceOf[Boolean]
  private var startCommandString: String = props.getOrElse("startCommand", "")


  private var cmdParams: java.util.List[String] = new java.util.ArrayList[String]()
  private var process: java.lang.Process = null
  private var builder: java.lang.ProcessBuilder = null

  private var lastFilesStatus: scala.collection.mutable.Map[String,Any] =  scala.collection.mutable.Map[String,Any]()
  private var t_lastFileStatus: Array[fileStats] = new Array[fileStats](numberOfConsumers)

  // If we kill an app while it's running, its sub-process should be killed too.
  Runtime.getRuntime().addShutdownHook(new Thread() {
    override def run() = {
      if (process != null) {
        process.destroy()
        process.waitFor()
      }
    }
  })

  override def run(): Unit = {
    var cmdArgs: Array[String] = startCommandString.split(" ")
    startProcess(cmdArgs)

    var keepRunning = true
    // Create Client
    while (zkClient == null) {
      zkClient = initZookeeper
      if (zkClient == null) Thread.sleep(refreshTime)
    }

    //CreateClient.CreateNodeIfNotExists(zkcConnectString, znodePath + "/" + URLEncoder.encode("TESTTEST","UTF-8"))
    //if(isTestMode) addToZK("TESTTEST", 50)
    //if(isTestMode) addToZK("TESTTEST2", 50)

    // Keep reading the contents
    while (keepRunning) {
      try {
        var isStuck: Boolean = false

        var files = zkClient.getChildren.forPath(znodePath)
        var trackingArray = files.toArray
        var iter = files.iterator()

        while(iter.hasNext) {
          var thisFile = iter.next()
          isStuck = checkIfStuck(thisFile)

          if (isStuck) {
            println(thisFile + " IS STUCK!!!!!!!!")
            restartProcess(cmdArgs)
          }
        }

        // Remove files from the lastFilesStatus if it is no longer in the zookeeper
        var i = 0
        var iter2 = t_lastFileStatus.iterator
        while(iter2.hasNext) {
          var file = iter2.next().asInstanceOf[fileStats]
          var isRelevant = false
          if (file != null) {
            trackingArray.foreach(currentFile => {
              if (file.fileName.equalsIgnoreCase(currentFile.asInstanceOf[String])) {
                isRelevant = true
              }
            })
            // if the file is not currently in zk, its irrelevant.. chuck it
            if (!isRelevant) {
              t_lastFileStatus(i) = null
            }
          }
          i += 1
        }

        Thread.sleep(refreshTime)
      } catch {
        case e: InterruptedException => {
          zkClient.close()
          keepRunning = false
        }
        case t: Throwable => {
          try {
            // report an exception
            Thread.sleep(refreshTime)
          } catch {
            case e: Throwable => {
              zkClient.close()
              keepRunning = false
            }
          }
        }
      }
    }
  }

  //
  private def checkIfStuck(fileName: String): Boolean = {

    var contents: String = null
    while (contents == null) {
      try {
        contents = new String(zkClient.getData().forPath(znodePath + "/" + fileName))
      } catch {
        case e: NoNodeException => {
          // here if the file is moved from underneath by the file processor.. not stuck for sure
          return false
        }
        case e: InterruptedException => {
          zkClient.close()
          throw e
        }
        case t: Throwable => {
          try {
            Thread.sleep(refreshTime)
          } catch {
            case e: Throwable => { throw e }
          }
        }
      }
    }

    var offsetData = contents.split(",")
    var thisTimestamp =  System.currentTimeMillis()

    var lastStatus: fileStats = null
    t_lastFileStatus.foreach(file => {
      if (file != null && file.fileName.equalsIgnoreCase(fileName)) {
        lastStatus = file
      }
    })

    // If this file is new, its obviously not stuck
    if (lastStatus == null) {
      var freeIndex = 0
      while(t_lastFileStatus(freeIndex) != null) freeIndex += 1
      if (freeIndex < t_lastFileStatus.size) t_lastFileStatus(freeIndex) = new fileStats(fileName, offsetData(0).toInt, thisTimestamp)
      return false
    }

    if ((offsetData(0).toInt == lastStatus.offset) && ((thisTimestamp - lastStatus.lastSeen) > maxTimeAllowedToBeStuck)) {
      return true
    }

    return false
  }

  private def initZookeeper: CuratorFramework = {
    try {
      zkcConnectString = props.getOrElse("ZOOKEEPER_CONNECT_STRING","")
      znodePath = props.getOrElse("ZNODE_PATH", "") + "/smartFileConsumer"

      CreateClient.CreateNodeIfNotExists(zkcConnectString, znodePath)
      return CreateClient.createSimple(zkcConnectString)
    } catch {
      case e: Exception => {
        println(e.printStackTrace())
        return null
      }
      case e: Throwable => {
        println(e.printStackTrace())
        return null
      }
    }
  }

  private def restartProcess(cmdArgs: Array[String]): Unit = {


    if (process != null) {
      process.destroy()
      process.waitFor()
    }

    startProcess(cmdArgs)
  }

  private def startProcess(cmdArgs: Array[String]) = {

    if (cmdParams.size == 0)
      cmdArgs.foreach( x => {cmdParams.add(x)})

    // Start the driver JVM
    if (builder == null) {
      builder = new java.lang.ProcessBuilder(cmdParams)
      val env = builder.environment()
      var log: File = new File("/tmp/test_log")
      builder.redirectOutput(Redirect.appendTo(log))
    }
    process = builder.start()
  }


  // For testing purposes only...
  private def addToZK (fileName: String, offset: Int, partitions: scala.collection.mutable.Map[Int,Int] = null) : Unit = {

    var zkValue: String = ""
    CreateClient.CreateNodeIfNotExists(zkcConnectString, znodePath + "/" + URLEncoder.encode(fileName,"UTF-8"))
    zkValue = zkValue + offset.toString

    // Set up Partition data
    if (partitions == null) {
      zkValue = zkValue + ",[]"
    } else {
      zkValue = zkValue + ",["
      var isFirst = true
      partitions.keySet.foreach(key => {
        if (!isFirst) zkValue = zkValue + ";"
        var mapVal = partitions(key)
        zkValue = zkValue + key.toString + ":" + mapVal.toString
        isFirst = false
      })
      zkValue = zkValue + "]"
    }

    zkClient.setData().forPath(znodePath + "/" + URLEncoder.encode(fileName,"UTF-8"), zkValue.getBytes)

  }

}

}