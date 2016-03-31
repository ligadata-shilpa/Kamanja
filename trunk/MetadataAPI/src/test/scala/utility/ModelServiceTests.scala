
package com.ligadata.MetadataAPI.utility.test

import java.io.ByteArrayInputStream

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
    assert(ModelService.addModelScala(getClass.getResource("/Metadata/model/HelloWorld.scala").getPath + "Invalid","helloworldmodel") == "File does not exist")
  }

  it should "add the scala model to the metadata" in {
    MessageService.addMessage(getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath)
    ConfigService.uploadCompileConfig(getClass.getResource("/Metadata/config/Model_Config_HelloWorld.json").getPath)
    ModelService.addModelScala(getClass.getResource("/Metadata/model/HelloWorld.scala").getPath,"helloworldmodel") should include regex ("\"Result Description\" : \"Model Added Successfully")
  }

  it should "add the scala model to the metadata based on user input" in {
   Console.setIn(new ByteArrayInputStream("1".getBytes))
     Console.setIn(new ByteArrayInputStream("1".getBytes))
    MessageService.addMessage(getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath)
    ConfigService.uploadCompileConfig(getClass.getResource("/Metadata/config/Model_Config_HelloWorld.json").getPath)
    ModelService.addModelScala("","helloworldmodel") should include regex ("\"Result Description\" : \"Model Added Successfully")
  }

  it should "give model configuration missing alert" in {
    ModelService.addModelScala(getClass.getResource("/Metadata/model/HelloWorld.scala").getPath,"") should include regex ("No model configuration loaded in the metadata")
  }

  "update model scala" should " successfully update a valid model" in {
    MessageService.addMessage(getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath)
    ConfigService.uploadCompileConfig(getClass.getResource("/Metadata/config/Model_Config_HelloWorld.json").getPath)
    ModelService.addModelScala(getClass.getResource("/Metadata/model/HelloWorld.scala").getPath,"helloworldmodel")
    ModelService.updateModelscala(getClass.getResource("/Metadata/model/HelloWorldV2.scala").getPath,"helloworldmodel") should include regex ("\"Result Description\" : \"Model Added Successfully")
  }

  it should " successfully update a valid model based on the user input" in {
    Console.setIn(new ByteArrayInputStream("2".getBytes))
    MessageService.addMessage(getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath)
    ConfigService.uploadCompileConfig(getClass.getResource("/Metadata/config/Model_Config_HelloWorld.json").getPath)
    ModelService.addModelScala(getClass.getResource("/Metadata/model/HelloWorld.scala").getPath,"helloworldmodel")
    ModelService.updateModelscala("","helloworldmodel") should include regex ("\"Result Description\" : \"Model Added Successfully")
  }

  "Add java model" should "add the java model to the metadata" in {
    MessageService.addMessage(getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath)
    ConfigService.uploadCompileConfig(getClass.getResource("/Metadata/config/Model_Config_HelloWorld.json").getPath)
    ModelService.addModelJava(getClass.getResource("/Metadata/model/HelloWorld.java").getPath,"helloworldmodel") should include regex ("\"Result Description\" : \"Model Added Successfully")
  }

  it should "add the java model to the metadata based on user input" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    MessageService.addMessage(getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath)
    ConfigService.uploadCompileConfig(getClass.getResource("/Metadata/config/Model_Config_HelloWorld.json").getPath)
    ModelService.addModelJava("","helloworldmodel") should include regex ("\"Result Description\" : \"Model Added Successfully")
  }

  it should "give model configuration missing alert" in {
    ModelService.addModelJava(getClass.getResource("/Metadata/model/HelloWorld.java").getPath,"") should include regex ("No model configuration loaded in the metadata")
  }

  it should "give incorrect file" in {
    ModelService.addModelJava(getClass.getResource("/Metadata/model/HelloWorld.java").getPath+" Invalid","") should include regex ("File does not exist")
  }

  "update model java" should " successfully update a valid model" in {
    MessageService.addMessage(getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath)
    ConfigService.uploadCompileConfig(getClass.getResource("/Metadata/config/Model_Config_HelloWorld.json").getPath)
    ModelService.addModelJava(getClass.getResource("/Metadata/model/HelloWorld.java").getPath,"helloworldmodel")
    ModelService.updateModeljava(getClass.getResource("/Metadata/model/HelloWorldV2.java").getPath,"helloworldmodel") should include regex ("\"Result Description\" : \"Model Added Successfully")
  }

  it should "update the java model to the metadata based on user input" in {
    Console.setIn(new ByteArrayInputStream("2".getBytes))
    MessageService.addMessage(getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath)
    ConfigService.uploadCompileConfig(getClass.getResource("/Metadata/config/Model_Config_HelloWorld.json").getPath)
    ModelService.addModelJava(getClass.getResource("/Metadata/model/HelloWorld.java").getPath,"helloworldmodel")
    ModelService.updateModeljava("","helloworldmodel") should include regex ("\"Result Description\" : \"Model Added Successfully")
  }

  "is valid dir" should " validate if the directory is present" in {
    assert(MessageService.IsValidDir(getClass.getResource("/Metadata/message").getPath)==true)
  }

  it should " invalidate a wrong directory path" in {
    assert(MessageService.IsValidDir(getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath+"Invalid")==false)
  }

}