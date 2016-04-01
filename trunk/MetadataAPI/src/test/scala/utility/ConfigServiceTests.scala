package com.ligadata.MetadataAPI.utility.test

import java.io.{File, ByteArrayInputStream}

import org.scalatest._
import com.ligadata.kamanja.metadata._
import com.ligadata.MetadataAPI._
import com.ligadata.MetadataAPI.Utility.ConfigService

/**
 * Created by dhavalkolapkar on 3/21/16.
 */
class ConfigServiceTests extends FlatSpec with Matchers with MetadataBeforeAndAfterEach {

  "add cluster config" should "add the valid cluster config" in {
    val clusterCfg = getClass.getResource("/Metadata/config/ClusterConfig.json").getPath
    val result = ConfigService.uploadClusterConfig(clusterCfg)
    result should include regex ("Uploaded Config successfully")
  }
  it should "add the user requested cluster config" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    val result = ConfigService.uploadClusterConfig("")
    result should include regex ("Uploaded Config successfully")
  }

  "uploadCompileConfig" should "upload the valid compile config" in {
    val compileCfg = getClass.getResource("/Metadata/config/Model_Config_HelloWorld.json").getPath
    val result = ConfigService.uploadCompileConfig(compileCfg)
    result should include regex ("Upload of model config successful")
  }

  it should "upload the user requested compile config" in {
    Console.setIn(new ByteArrayInputStream("2".getBytes))
      ConfigService.uploadCompileConfig("") should include regex ("Upload of model config successful")
  }

 /* "dumpAllCfgObjects" should "display the configs " in {
    val compileCfg = getClass.getResource("/Metadata/config/Model_Config_HelloWorld.json").getPath
     ConfigService.uploadCompileConfig(compileCfg)
    ConfigService.dumpAllCfgObjects should include regex "Successfully fetched all configs"
  }*/

  "is valid dir" should " validate if the directory is present" in {
    val msgDef = getClass.getResource("/Metadata/message").getPath
    val result=ConfigService.IsValidDir(msgDef)
    assert(result==true)
  }

  it should " invalidate a wrong directory path" in {
    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath+"Invalid"
    val result=ConfigService.IsValidDir(msgDef)
    assert(result==false)
  }

  "user input " should "get the user input and perform requested operation on it" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    val messages: Array[File] = new java.io.File(getClass.getResource("/Metadata/inputMessage").getPath).listFiles.filter(_.getName.endsWith(".json"))
    ConfigService.getUserInputFromMainMenu(messages).size > 0
  }
}

