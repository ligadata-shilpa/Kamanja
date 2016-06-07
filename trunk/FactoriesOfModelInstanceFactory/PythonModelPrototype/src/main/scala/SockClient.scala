#!/bin/bash
exec scala "$0" "$@"
!#



import java.net._
import java.io._
import java.nio.ByteBuffer
import java.security.MessageDigest

import scala.collection.JavaConverters._

import scala.io._
import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._


object SockClient {

    /** cmdMap contains command preparation objects for each command type */
    val cmdMap : Map[String, PyCmd] =
        List[(String,PyCmd)](
            ("addModel" , new AddModelCmd("addModel"))
            ,("removeModel" , new RemoveModelCmd("removeModel"))
            ,("serverStatus" , new ServerStatusCmd("serverStatus"))
            ,("executeModel" , new ExecuteModelCmd("executeModel"))
            ,("startServer" , new StartServerCmd("startServer"))
            ,("stopServer" , new StopServerCmd("stopServer"))
        ).toMap

    /** Usage message for console display */
    def usage: String = {
        """
    One of the following must be supplied.

  SocketClient.scala <named args...>
  where <named args> are:

    --cmd startServer [--host <hostname or ip> ... default = localhost]
                      [--port <user port no.> ... default=9999]
    --cmd stopServer  [--host <hostname or ip> ... default = localhost]
                      [--port <user port no.> ... default=9999]
    --cmd addModel     --filePath <filePath>
    --cmd removeModel  --modelName '<modelName>'
    --cmd serverStatus
    --cmd executeModel --modelName '<modelName>'
                       --msg '<msg data>'
    --cmd executeModel --modelName '<modelName>'
                       --filePath '<msg file path>'


  It is possible to run multiple servers on same host simply by varying 
  the port number.
        """
    }


    def main (args : Array[String]) {
        val arglist = args.toList
        if (args.isEmpty) {
            println(usage)
            sys.exit(0)
        }

        type OptionMap = Map[Symbol, String]
        def nextOption(map: OptionMap, list: List[String]): OptionMap = {
            list match {
                case Nil => map
                case "--cmd" :: value :: tail =>
                    nextOption(map ++ Map('cmd -> value), tail)
                case "--filePath" :: value :: tail =>
                    nextOption(map ++ Map('filePath -> value), tail)
                case "--modelName" :: value :: tail =>
                    nextOption(map ++ Map('modelName -> value), tail)
                case "--msg" :: value :: tail =>
                    nextOption(map ++ Map('msg -> value), tail)
                case "--host" :: value :: tail =>
                    nextOption(map ++ Map('host -> value), tail)
                case "--port" :: value :: tail =>
                    nextOption(map ++ Map('port -> value), tail)
                case "--user" :: value :: tail =>
                    nextOption(map ++ Map('user -> value), tail)
                case option :: tail => println("Unknown option " + option)
                    sys.exit(1)
            }
        }
        val options = nextOption(Map(), arglist)
        val cmd : String = if (options.contains('cmd)) options.apply('cmd) else null
        val filePath : String = if (options.contains('filePath)) options.apply('filePath) else null
        val modelName : String = if (options.contains('modelName)) options.apply('modelName) else null
        val msg : String = if (options.contains('msg)) options.apply('msg) else null
        val user : String = if (options.contains('user)) options.apply('user) else "rich"
        val host : String = if (options.contains('host)) options.apply('host) else "localhost"
        val portNo : Int = if (options.contains('port)) options.apply('port).toInt else 9999

        val ok : Boolean = cmd != null
        if (! ok) {
            println("\nInvalid command..\n")
            println(usage)
            sys.exit(1)
        }

        /** known command? */
        val cmdObj : PyCmd = cmdMap.getOrElse(cmd, null)
        if (cmdObj == null) {
            println("\nCommand not recognized...\n")
            println(usage)
            sys.exit(1)
        }

        /** command specific checks */
        val (cmdOk,errMsg) : (Boolean,String) =
            cmdObj.semanticCheck(filePath
                , modelName
                , msg
                , user
                , host
                , portNo)
        if (! cmdOk) {
            println(s"\nCommand $cmd parameters are invalid...$errMsg\n")
            println(usage)
            sys.exit(1)
        }


        if (cmd == "startServer") {
            val startServerCmd : StartServerCmd = cmdObj.asInstanceOf[StartServerCmd]
            val result : String = startServerCmd.startServer(filePath, user, host, portNo)
            println(s"Server started? $result")
        } else {
	        /** Prepare the command message for transport (except for the case of the
	          StartServerCmd instance... */
	        val cmdMsg : Array[Byte] = cmdObj.prepareCmdMsg(filePath
	            , modelName
	            , msg
	            , user
	            , host
	            , portNo)
            /** send the command to the server for execution */
            val inetbyname = InetAddress.getByName(host)
            println("inetbyname = " + inetbyname)
            val s : Socket = new Socket(inetbyname, portNo)
            lazy val in = new DataInputStream(s.getInputStream)
            //lazy val in = new BufferedSource(s.getInputStream).getLines()
            val out = new DataOutputStream(s.getOutputStream)
            //val out = new PrintStream(s.getOutputStream)

            //out.println(cmdMsg)
            val cmdLen : Int = cmdMsg.length
            out.write(cmdMsg, 0, cmdLen)
            out.flush()
            
            //val resp : String = in.next
            val buffer : Array[Byte] = new Array[Byte](2^16) // 64k
            val answeredBytes : ArrayBuffer[Byte] = ArrayBuffer[Byte]()
            var bytesReceived = in.read(buffer)
            while (bytesReceived > 0) {
        		answeredBytes ++= buffer
            }
            
            val response : String = unpack(answeredBytes)
            println(response)

            s.close()
        }
        sys.exit(0)
    }

    /** 
    Peel off the md5 hash from front and back of the answered bytes, verifying
    they are a) the same value and b) reflect the md5 hash of the payload string.
    If satisfactory, reconstitute the json string value from the payload portion.

	@param answeredBytes an ArrayBuffer containing the reply from the py server
	@return the string result if successfully transmitted.  When result integrity
		an issue, issue error message as the result revealing the finding.

    */
    def unpack(answeredBytes : ArrayBuffer[Byte]) : String = {
    	val lenOfMd5Digest : Int = 16
    	val lenOfSha256Digest : Int = 32
    	val lenOfDigest : Int = lenOfMd5Digest
    	val lenOfInt : Int = 4

    	val reasonable : Boolean = answeredBytes != null && answeredBytes.length > 2 * lenOfDigest + lenOfInt
    	val answer : String = if (reasonable) {
    		val byteBuffer :  ByteBuffer = ByteBuffer.wrap(answeredBytes.toArray)
    		val startDigest : scala.Array[Byte] = new scala.Array[Byte](lenOfMd5Digest)
    		val endDigest : scala.Array[Byte] = new scala.Array[Byte](lenOfMd5Digest)
    		/** unpack the byte array into md5 digest, payload len, payload, md5 digest */
			byteBuffer.get(startDigest,byteBuffer.arrayOffset(),lenOfMd5Digest)
    		val payloadLen : Int = byteBuffer.getInt()
    		var payloadArray : scala.Array[Byte] = new scala.Array[Byte](payloadLen)
			byteBuffer.get(payloadArray,byteBuffer.arrayOffset(),payloadLen)
			byteBuffer.get(endDigest,byteBuffer.arrayOffset(),lenOfMd5Digest)

			val digestsMatch : Boolean = startDigest.sameElements(endDigest)
			val digestedMsg : String = if (digestsMatch) {
				/** test the payload to see if the same digest can be obtained from it */
				val md : MessageDigest = MessageDigest.getInstance("MD5");
				md.update(payloadArray)
				val mddigest : Array[Byte] = md.digest()
				val integrityVerified : Boolean = mddigest.sameElements(endDigest)
				val verifiedMsg : String = if (integrityVerified) {
					payloadArray.toString
				} else {
					"while begin and end digests match, they don't reflect md5 digest of the bytes answered."
				} 
				verifiedMsg
			} else {
				"the beginning and ending digests do not match... results are bogus"
			}
			digestedMsg
    	} else {
    		"unreasonable bytes returned... either null or 0 bytes"
    	}
 		answer
    }
}

/** Abstract class for objects that handle semantic checks and command preparation for
  * the server to execute.
  */
abstract class PyCmd (val cmd : String) {
    def semanticCheck(filePath : String, modelName : String, msg : String, user : String, host : String, portNo : Int) : (Boolean,String)
    def prepareCmdMsg(filePath : String, modelName : String, msg : String, user : String, host : String, portNo : Int) : scala.Array[Byte]
}
/** Commands:
  * StartServerCmd, StopServerCmd, AddModelCmd, RemoveModelCmd,
  * ServerStatusCmd, ExecuteModelCmd
  */


/**
  * StartServerCmd
  **
  *FIXME: SSH
  *The StartServerCmd can be started on a remote host if desired (*_in the not too distant
  *future that is_*).  SSH will be used whenever the --host parameter is specified with a value other than _localhost_.  For this version it is expected that the
  *filePath parameter contains the path of the server on that system _and_ that
  *the PYTHONPATH refers to that path.
  **
  *All of the models that are _added_ to the server will be placed in a
  *subdirectory called _*models*_ that is expected to be in the directory.
  *The command will fail if it is not present.
  **
  *For now, the server runs on the localhost.
  *
  */
class StartServerCmd(cmd : String) extends PyCmd(cmd) {

    override def semanticCheck(filePath : String
                               , modelName : String
                               , msg : String
                               , user : String
                               , host : String
                               , portNo : Int) : (Boolean,String) = {

        val isLocalHost : Boolean = host == "localhost"
        val errMsg : String = if (isLocalHost) "" else "only local host supported for this build"
        (isLocalHost, errMsg)
    }

    /**
      *
      */
    override def prepareCmdMsg(filePath : String
                               , modelName : String
                               , msg : String
                               , user : String
                               , host : String
                               , portNo : Int) : scala.Array[Byte] = {
        new scala.Array[Byte](0)
    }

    /**
      * FIXME: the pyserver file should be installed as part of the Kamanja installation.  It does not get copied in
      * total in that it is just the top level of a bunch of command modules (e.g., addModel, serverStatus, et al).
      * To modify the server on the fly is still possible in that these commands really provide the behavior.
      *
      * What we really need to do is treat the py server commands like the models... i.e., make it possible to add
      * them and/or replace them.  A restart mechanism is needed.  I believe it is possible to cause the modules to be
      * reloaded... we need to look into that.
      *
      * For the near future...
      * Invoke a script that is part of the installation.  In that script, the PYTHONPATH is set and the location of
      * the pyserver to run is known.
      *
      * @param filePath
      * @param user
      * @param host
      * @param portNo
      * @return
      */
    def startServer(filePath : String, user : String, host : String, portNo : Int) : String = {

        val useSSH : Boolean = host != "localhost"

        val cmdSeq : Seq[String] = if (useSSH) {
            /** FIXME: send the file to the appropriate directory ...
              *assume that the directory given is in the PYTHONPATH
              *on the target machine... then
              *send the following command */
              /** hack: assume same location of script on all systems */
            val scriptLocation : String = sys.env("PYTHONSERVERSCRIPT")
            val cmd : String = s"$scriptLocation/StartPythonServer.sh $host ${portNo.toString}"
            val userMachine : String = s"$user@$host"
            val remoteCmd : String = s"$scriptLocation/StartPythonServer.sh $host ${portNo.toString}"
            Seq[String]("ssh", userMachine, remoteCmd)
        } else {
            val portStr = s"${portNo.toString} "
            val scriptLocation : String = sys.env("PYTHONSERVERSCRIPT")
            val cmd : String = s"${scriptLocation}/StartPythonServer.sh"
            println(s"Invoking script $cmd  $host $portStr to start the python server")
            Seq[String](cmd, host, portStr)
        }

        val startResult : Int = if (useSSH) {
            Process(cmdSeq).!
        } else {
            val seqList = cmdSeq.toList
            val seq = seqList.toSeq
            val seqCmd = cmdSeq.toString

            /** run will launch the server program in the background ... what we want in this case... can't use .! which waits for program completion. */
            val pySrvCmd = Process(cmdSeq)
            pySrvCmd.run

            /** If the script were to finish while we waited, we could use this which can collect the stdout and stderr from the execution as well as return the
            script return code.

            val (rc,stdOut, stdErr) : (Int, String, String) = runCmdCollectOutput(cmdSeq)
            println(s"seqCmd = $seqCmd")
            println(s"stdOut = $stdOut")
            println(s"stdErr = $stdErr")
            rc*/
            0
        }

        val resultStr : String = if (startResult == 0) "Successfully" else "Failed"
        resultStr
    }

    /**
      * Execute the supplied command sequence. Answer with the rc, the stdOut, and stdErr outputs from
      * the external command represented in the sequence.
      *
      * Warning: you must wait for this process to end.  It is **_not_** to be used to launch a daemon. Use
      * cmd.run instead. If this application is itself a server, you can run it with the ProcessLogger as done
      * here ... possibly with a different kind of underlying stream that writes to a log file or in some fashion
      * consumable with the program.
      *
      * @param cmd external command sequence
      * @return (rc, stdout, stderr)
      */
    def runCmdCollectOutput(cmd: Seq[String]): (Int, String, String) = {
        val stdoutStream = new ByteArrayOutputStream
        val stderrStream = new ByteArrayOutputStream
        val stdoutWriter = new PrintWriter(stdoutStream)
        val stderrWriter = new PrintWriter(stderrStream)
        val exitValue = cmd.!(ProcessLogger(stdoutWriter.println, stderrWriter.println))
        stdoutWriter.close()
        stderrWriter.close()
        (exitValue, stdoutStream.toString, stderrStream.toString)
    }


}

/** StopServerCmd */
class StopServerCmd(cmd : String) extends PyCmd(cmd) {

    /** FIXME: do meaningful checks here */
    override def semanticCheck(filePath : String
                               , modelName : String
                               , msg : String
                               , user : String
                               , host : String
                               , portNo : Int) : (Boolean,String) = {
        (true,"")
    }

    override def prepareCmdMsg(filePath : String
                               , modelName : String
                               , msg : String
                               , user : String
                               , host : String
                               , portNo : Int) : scala.Array[Byte] = {


		//val md : MessageDigest = MessageDigest.getInstance("SHA1");
		val md : MessageDigest = MessageDigest.getInstance("MD5");
		md.update(cmd.getBytes)
		val mddigest : scala.Array[Byte] = md.digest()
		val payload : scala.Array[Byte] = cmd.getBytes
		val sizeOfInt : Int = 4
		var lenBytes : ByteBuffer = ByteBuffer.allocate(sizeOfInt)
		lenBytes.putInt(payload.length)
		val payloadLenAsBytes : scala.Array[Byte] = lenBytes.array()
		val cmdBytes : scala.Array[Byte] = mddigest ++
									payloadLenAsBytes ++
									payload ++
                                    mddigest
		cmdBytes
        
    }
}

/**
  * AddModelCmd
  *
  *  NOTE about the model name.  For this prototype, the name of the python program to be sent to the
  *  server *_must be_* the name of the python program file.  It is this file name that is sought by the loader
  *  in the server to load the python program into the server (as if it were an explicit import).
  *
  *  For this reason, when a command is invoked that refers to the model (e.g.,
  *     --cmd removeModel 'modelName'
  *     --cmd executeModel --modelName '<modelName>' --msg 'msg data'
  *  the _modelName_ value must be the name of the appropriate python program that was previously added with
  *  the addModel command.
  *
  *  Failure to supply a path in the form _some/path/mymodel.py_ for your file path will likely cause a
  *  RuntimeException and command failure.
  *
  */
class AddModelCmd(cmd : String) extends PyCmd(cmd) {

    /** FIXME: do meaningful checks here */
    override def semanticCheck(filePath : String
                               , modelNm : String
                               , msg : String
                               , user : String
                               , host : String
                               , portNo : Int) : (Boolean,String) = {
        (true,"")
    }

    /**
      * Determine the model name - the key - for the server dispatch map.
      * Print it to the console and pass it and the model source file content
      * to the server.
      */
    override def prepareCmdMsg(filePath : String
                               , modelNm : String
                               , msg : String
                               , user : String
                               , host : String
                               , portNo : Int) : Array[Byte] = {

        val modelName : String = if (filePath != null && filePath.contains('.')) {
            val fileName : String = filePath.split('/').last
            fileName.split('.').dropRight(1).last // drop the .py
        } else {
            printf("the model file path is bogus... things are going to fail.")
            throw new RuntimeException("the model file path doesn't appear to have a legitimate python file path in it... can't determine the model name")
        }

        val modelSrcPath : String = Source.fromFile(filePath).mkString
        val payloadStr : String = s"$cmd\n$modelName$modelSrcPath"
		//val md : MessageDigest = MessageDigest.getInstance("SHA1");
		val md : MessageDigest = MessageDigest.getInstance("MD5");
		md.update(payloadStr.getBytes)
		val mddigest : Array[Byte] = md.digest()
		val payload : Array[Byte] = payloadStr.getBytes
		val sizeOfInt : Int = 4
		var lenBytes : ByteBuffer = ByteBuffer.allocate(sizeOfInt)
		lenBytes.putInt(payload.length)
		val payloadLenAsBytes : Array[Byte] = lenBytes.array()
		val cmdBytes : Array[Byte] = mddigest ++
									 payloadLenAsBytes ++ 
									 payload ++ 
									 mddigest
		cmdBytes
    }
}

/** RemoveModelCmd */
class RemoveModelCmd(cmd : String) extends PyCmd(cmd) {

    /** FIXME: do meaningful checks here */
    override def semanticCheck(filePath: String
                               , modelName: String
                               , msg: String
                               , user: String
                               , host: String
                               , portNo: Int) : (Boolean,String) = {
        (true,"")
    }

    override def prepareCmdMsg(filePath: String
                               , modelName: String
                               , msg: String
                               , user: String
                               , host: String
                               , portNo: Int): Array[Byte] = {

        val payloadStr : String = s"$cmd\n$modelName"
		//val md : MessageDigest = MessageDigest.getInstance("SHA1");
		val md : MessageDigest = MessageDigest.getInstance("MD5");
		md.update(payloadStr.getBytes)
		val mddigest : Array[Byte] = md.digest()
		val payload : Array[Byte] = payloadStr.getBytes
		val sizeOfInt : Int = 4
		var lenBytes : ByteBuffer = ByteBuffer.allocate(sizeOfInt)
		lenBytes.putInt(payload.length)
		val payloadLenAsBytes : Array[Byte] = lenBytes.array()
		val cmdBytes : Array[Byte] = mddigest ++
									 payloadLenAsBytes ++ 
									 payload ++ 
									 mddigest
		cmdBytes
    }
}

/** ServerStatusCmd */
class ServerStatusCmd(cmd: String) extends PyCmd(cmd) {

    /** FIXME: do meaningful checks here */
    override def semanticCheck(filePath: String
                               , modelName: String
                               , msg: String
                               , user: String
                               , host: String
                               , portNo: Int) : (Boolean,String) = {
        (true,"")
    }

    override def prepareCmdMsg(filePath: String
                               , modelName: String
                               , msg: String
                               , user: String
                               , host: String
                               , portNo: Int) : Array[Byte] = {
        val payloadStr : String = cmd
		//val md : MessageDigest = MessageDigest.getInstance("SHA1");
		val md : MessageDigest = MessageDigest.getInstance("MD5");
		md.update(payloadStr.getBytes)
		val mddigest : Array[Byte] = md.digest()
		val payload : Array[Byte] = payloadStr.getBytes
		val sizeOfInt : Int = 4
		var lenBytes : ByteBuffer = ByteBuffer.allocate(sizeOfInt)
		lenBytes.putInt(payload.length)
		val payloadLenAsBytes : Array[Byte] = lenBytes.array()
		val cmdBytes : Array[Byte] = mddigest ++
									 payloadLenAsBytes ++ 
									 payload ++ 
									 mddigest
		cmdBytes
    }
}

/** ExecuteModelCmd */
class ExecuteModelCmd(cmd : String) extends PyCmd(cmd) {

    /** FIXME: do meaningful checks here */
    override def semanticCheck(filePath : String
                               , modelName : String
                               , msg : String
                               , user : String
                               , host : String
                               , portNo : Int) : (Boolean,String) = {
        (true,"")
    }

    override def prepareCmdMsg(filePath : String
                               , modelName : String
                               , msg : String
                               , user : String
                               , host : String
                               , portNo : Int) : Array[Byte] = {

    	val cmdStr : String = if (filePath != null) {
        	val msgs : String = Source.fromFile(filePath).mkString
        	s"$cmd\n$modelName\n$msgs"

		} else {
        	s"$cmd\n$modelName\n$msg"			
		}
        val payloadStr : String = cmdStr
		//val md : MessageDigest = MessageDigest.getInstance("SHA1");
		val md : MessageDigest = MessageDigest.getInstance("MD5");
		md.update(payloadStr.getBytes)
		val mddigest : Array[Byte] = md.digest()
		val payload : Array[Byte] = payloadStr.getBytes
		val sizeOfInt : Int = 4
		var lenBytes : ByteBuffer = ByteBuffer.allocate(sizeOfInt)
		lenBytes.putInt(payload.length)
		val payloadLenAsBytes : Array[Byte] = lenBytes.array()
		val cmdBytes : Array[Byte] = mddigest ++
									 payloadLenAsBytes ++ 
									 payload ++ 
									 mddigest
		cmdBytes
    }

}

SockClient.main(args)