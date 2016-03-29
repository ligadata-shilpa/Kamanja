package com.ligadata.MetadataAPI.utility.test
import org.scalatest._
import play.api.libs.json._
import com.ligadata.kamanja.metadata._
import com.ligadata.MetadataAPI._
import com.ligadata.MetadataAPI.Utility.MessageService

/**
 * Created by dhavalkolapkar on 3/21/16.
 */
class MessageServiceTests extends FlatSpec with Matchers with MetadataBeforeAndAfterEach {

  "addMessage" should "produce error when invalid path to msg def is provided" in {
    //val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld3.json").getPath
    //assert(results == "Message defintion file does not exist")
  }

  it should "produce  message def" in {

    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    val result = MessageService.addMessage(msgDef)
    val response = Json.parse(result)
    val resultDesc = (response \ "APIResults" \ "Result Description").result.as[String]
    resultDesc should startWith regex "Message Added Successfully"
  }
/*
  it should "produce Higer version present when same or lower version added" in {

    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    var result = MessageService.addMessage(msgDef)
    result = MessageService.addMessage(msgDef)
    val response = Json.parse(result)
    val resultDesc = (response \ "APIResults" \ "Result Description").result.as[String]
    resultDesc should startWith regex "Error: com.ligadata.Exceptions.MsgCompilationFailedException: Higher active version of Message"
  }


  "delete message" should "delete the requested message definition from the metadata" in {
    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    MessageService.addMessage(msgDef)
    val result =MessageService.removeMessage("com.ligadata.kamanja.samples.messages.msg1.000000000001000000")
    val response = Json.parse(result)
    val resultDesc = (response \ "APIResults" \ "Result Description").result.as[String]
    resultDesc should startWith regex "Deleted Message Successfully"
  }

  it should " produce error if invalid message definition provided for deletion" in {
    val result =MessageService.removeMessage("system.invalid_msg_def.00000000000100001")
    val response = Json.parse(result)
    val resultDesc = (response \ "APIResults" \ "Result Description").result.as[String]
    resultDesc should startWith regex "Failed to Delete Message. Message Not found:"
  }


  "get message " should " retrieve the requested message definition" in {
    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    MessageService.addMessage(msgDef)
    val result= MessageService.getMessage("com.ligadata.kamanja.samples.messages.msg1.000000000001000000")
    val response = Json.parse(result)
    val resultDesc = (response \ "APIResults" \ "Result Description").result.as[String]
    resultDesc should startWith regex "Successfully fetched message from cache"
  }

  "get message " should "fail to retrieve invalid(not present in Metadata) message definition " in {
    val result= MessageService.getMessage("system.invalid_msg_def.000000000001000000")
    val response = Json.parse(result)
    val resultDesc = (response \ "APIResults" \ "Result Description").result.as[String]
    resultDesc should startWith regex "Failed to fetch message from cache"
  }

  "get all messages" should " return the list of messages in the metadata" in {
    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    MessageService.addMessage(msgDef)
    val result= MessageService.getAllMessages
    val response = Json.parse(result)
    //println(response)
    val resultDesc = (response \ "APIResults" \ "Result Description").result.as[String]
    resultDesc should startWith regex "Successfully retrieved all the messages"
  }

  it should "produce NoSuchElementException when head is invoked" in {
    intercept[NoSuchElementException] {
    Set.empty.head
    }
  }*/
}
