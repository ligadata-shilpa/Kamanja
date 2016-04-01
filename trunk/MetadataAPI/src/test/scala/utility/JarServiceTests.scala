package com.ligadata.MetadataAPI.utility.test

import java.io.ByteArrayInputStream

import org.scalatest._
import com.ligadata.kamanja.metadata._
import com.ligadata.MetadataAPI._
import com.ligadata.MetadataAPI.Utility.JarService
import java.io.File

/**
 * Created by dhavalkolapkar on 3/21/16.
 */
class JarServiceTests extends FlatSpec with Matchers with MetadataBeforeAndAfterEach {

  "uploadJar" should "upload the valid jar" in {
    JarService.uploadJar(getClass.getResource("/jars/lib/system/activation-1.1.jar").getPath) should include regex ("Uploaded Jar successfully")
  }

  it should "upload the jar selected at runtime by the user" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    JarService.uploadJar("") should include regex ("Uploaded Jar successfully")
  }

  "uploadJars" should "upload the jars based on the user selection from the runtime list" in {
    //var files: Array[File]=new
    val files=new Array[File](1)
    files(0)=new File(getClass.getResource("/jars/lib/system/activation-1.1.jar").getPath)
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    JarService.uploadJars(files) should include regex ("Uploaded Jar successfully")
  }

  "is valid dir" should " validate if the directory is present" in {
    assert((JarService.IsValidDir(getClass.getResource("/Metadata/message").getPath))==true)
  }

  it should " invalidate a wrong directory path" in {
    assert((JarService.IsValidDir(getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath+"Invalid"))==false)
  }
}

