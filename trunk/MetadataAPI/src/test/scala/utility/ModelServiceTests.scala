package com.ligadata.MetadataAPI.utility.test

import org.scalatest._
import play.api.libs.json._
import com.ligadata.kamanja.metadata._
import com.ligadata.MetadataAPI._
import com.ligadata.MetadataAPI.Utility.MessageService
import com.ligadata.MetadataAPI.Utility.ModelService
import com.ligadata.MetadataAPI.Utility.ConfigService
/**
 * Created by dhavalkolapkar on 3/22/16.
 */
class ModelServiceTests extends FlatSpec with Matchers {

  "add model scala" should "produce error when invalid path to model def is provided" in {
    val results = ModelService.addModelScala("../../resources/Metadata/model/HellowWorld.scala")
    assert(results == "File does not exist")
  }

  it should "add the scala model to the metadata" in {
    val metadataConfig=getClass.getResource("/Metadata/config/MetadataAPI.properties").getPath
    MetadataAPIImpl.InitMdMgrFromBootStrap(metadataConfig, false)

    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    val msg=MessageService.addMessage(msgDef)
    println("Msg added "+msg)
    val modelConfig=getClass.getResource("/Metadata/config/Model_Config_HelloWorld.json").getPath
    val compileConfig=ConfigService.uploadCompileConfig(modelConfig)
    println("compile config added: "+compileConfig)

    val modelDef = getClass.getResource("/Metadata/model/HelloWorld.scala").getPath
    //println(config)
    val result = ModelService.addModelScala(modelDef,"helloworldmodel")
    MessageService.removeMessage("com.ligadata.kamanja.samples.messages.msg1.000000000001000000")
    ModelService.removeModel("com.ligadata.samples.models.helloworldmodel.000000000000000001")
    MetadataAPIImpl.shutdown
    val response = Json.parse(result)
    val resultDesc = (response \ "APIResults" \ "Result Description").result.as[String]
    resultDesc should startWith regex "Model Added Successfully"
  }


  
}
