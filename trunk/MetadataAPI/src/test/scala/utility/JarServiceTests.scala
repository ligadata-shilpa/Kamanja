package com.ligadata.MetadataAPI.utility.test
import org.scalatest._
import com.ligadata.kamanja.metadata._
import com.ligadata.MetadataAPI._
import com.ligadata.MetadataAPI.Utility.JarService

/**
 * Created by dhavalkolapkar on 3/21/16.
 */
class JarServiceTests extends FlatSpec with Matchers with MetadataBeforeAndAfterEach {

  "uploadJar" should "upload the valid jar" in {
    JarService.uploadJar(getClass.getResource("/jars/lib/system/activation-1.1.jar").getPath) should include regex ("Uploaded Jar successfully")
  }

  "uploadJars" should "upload the valid jars" in {

  }

  "is valid dir" should " validate if the directory is present" in {
    assert((JarService.IsValidDir(getClass.getResource("/Metadata/message").getPath))==true)
  }

  it should " invalidate a wrong directory path" in {
    assert((JarService.IsValidDir(getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath+"Invalid"))==false)
  }
}

