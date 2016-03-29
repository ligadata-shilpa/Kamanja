package com.ligadata.MetadataAPI.utility.test


import org.scalatest._
import play.api.libs.json._
import com.ligadata.kamanja.metadata._
import com.ligadata.MetadataAPI._
import com.ligadata.MetadataAPI.Utility.MessageService
import com.ligadata.MetadataAPI.Utility.ModelService
import com.ligadata.MetadataAPI.Utility.ConfigService
/**
 * Created by dhavalkolapkar on 3/23/16.
 */
class TestMetadataConfig  extends FlatSpec with Matchers with MetadataBeforeAndAfterEach{

  "add message" should "produce  message def" in {


/* val metadataConfig=getClass.getResource("/Metadata/config/MetadataAPI.properties").getPath
    MetadataAPIImpl.InitMdMgrFromBootStrap(metadataConfig, false)
*/
    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    val result = MessageService.addMessage(msgDef)
    MetadataAPIImpl.shutdown
    val response = Json.parse(result)
    println(result)
    val resultDesc = (response \ "APIResults" \ "Result Description").result.as[String]
    resultDesc should startWith regex "Message Added Successfully"
  }
}
