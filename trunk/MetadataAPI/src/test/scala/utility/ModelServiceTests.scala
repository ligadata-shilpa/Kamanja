
package com.ligadata.MetadataAPI.utility.test

import org.scalatest._
import com.ligadata.kamanja.metadata._
import com.ligadata.MetadataAPI._
import com.ligadata.MetadataAPI.Utility.MessageService
import com.ligadata.MetadataAPI.Utility.ModelService
import com.ligadata.MetadataAPI.Utility.ConfigService
import com.ligadata.MetadataAPI.MetadataAPI.ModelType
/**
 * Created by dhavalkolapkar on 3/22/16.
 */
class ModelServiceTests extends FlatSpec with Matchers with MetadataBeforeAndAfterEach {
  "add model scala" should "produce error when invalid path to model def is provided" in {
    val modelDef = getClass.getResource("/Metadata/model/HelloWorld.scala").getPath + "Invalid"
    val result = ModelService.addModelScala(modelDef,"helloworldmodel")
    assert(result == "File does not exist")
  }

  it should "add the scala model to the metadata" in {
    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    val msg=MessageService.addMessage(msgDef)
    println("Msg added "+msg)
    val modelConfig=getClass.getResource("/Metadata/config/Model_Config_HelloWorld.json").getPath
    val compileConfig=ConfigService.uploadCompileConfig(modelConfig)
    println("compile config added: "+compileConfig)
    val modelDef = getClass.getResource("/Metadata/model/HelloWorld.scala").getPath
    val result = ModelService.addModelScala(modelDef,"helloworldmodel")
    result should include regex ("\"Result Description\" : \"Model Added Successfully")
  }

  it should "give model configuration missing alert" in {
    val modelDef = getClass.getResource("/Metadata/model/HelloWorld.scala").getPath
    val result = ModelService.addModelScala(modelDef,"")
    result should include regex ("No model configuration loaded in the metadata")
  }

  "update model scala" should " successfully update a valid model" in {
    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    val msg=MessageService.addMessage(msgDef)
    println("Msg added "+msg)
    val modelConfig=getClass.getResource("/Metadata/config/Model_Config_HelloWorld.json").getPath
    val compileConfig=ConfigService.uploadCompileConfig(modelConfig)
    println("compile config added: "+compileConfig)
    var modelDef = getClass.getResource("/Metadata/model/HelloWorld.scala").getPath
    ModelService.addModelScala(modelDef,"helloworldmodel")
    modelDef = getClass.getResource("/Metadata/model/HelloWorldV2.scala").getPath
    val result = ModelService.updateModelscala(modelDef,"helloworldmodel")
    result should include regex ("\"Result Description\" : \"Model Added Successfully")
  }

  "Add java model" should "add the java model to the metadata" in {
    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    val msg=MessageService.addMessage(msgDef)
    println("Msg added "+msg)
    val modelConfig=getClass.getResource("/Metadata/config/Model_Config_HelloWorld.json").getPath
    val compileConfig=ConfigService.uploadCompileConfig(modelConfig)
    println("compile config added: "+compileConfig)
    val modelDef = getClass.getResource("/Metadata/model/HelloWorld.java").getPath
    val result = ModelService.addModelJava(modelDef,"helloworldmodel")
    result should include regex ("\"Result Description\" : \"Model Added Successfully")
  }

  it should "give model configuration missing alert" in {
    val modelDef = getClass.getResource("/Metadata/model/HelloWorld.java").getPath
    val result = ModelService.addModelJava(modelDef,"")
    result should include regex ("No model configuration loaded in the metadata")
  }

  it should "give incorrect file" in {
    val modelDef = getClass.getResource("/Metadata/model/HelloWorld.java").getPath+" Invalid"
    val result = ModelService.addModelJava(modelDef,"")
    result should include regex ("File does not exist")
  }

  "update model java" should " successfully update a valid model" in {
    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    val msg=MessageService.addMessage(msgDef)
    println("Msg added "+msg)
    val modelConfig=getClass.getResource("/Metadata/config/Model_Config_HelloWorld.json").getPath
    val compileConfig=ConfigService.uploadCompileConfig(modelConfig)
    println("compile config added: "+compileConfig)
    var modelDef = getClass.getResource("/Metadata/model/HelloWorld.java").getPath
    ModelService.addModelJava(modelDef,"helloworldmodel")
    modelDef = getClass.getResource("/Metadata/model/HelloWorldV2.java").getPath
    val result = ModelService.updateModeljava(modelDef,"helloworldmodel")
    result should include regex ("\"Result Description\" : \"Model Added Successfully")
  }
}