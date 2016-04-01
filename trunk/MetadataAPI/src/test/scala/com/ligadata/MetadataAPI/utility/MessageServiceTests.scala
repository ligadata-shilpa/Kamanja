
package com.ligadata.MetadataAPI.utility
import java.io.{File, ByteArrayInputStream}
import org.scalatest._
import com.ligadata.kamanja.metadata._
import com.ligadata.MetadataAPI._
import com.ligadata.MetadataAPI.Utility.MessageService

/**
 * Created by dhavalkolapkar on 3/21/16.
 */

class MessageServiceTests extends FlatSpec with Matchers with MetadataBeforeAndAfterEach {

  "addMessage" should "produce error when invalid path to msg def is provided" in {
    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath +" Invalid"
    val result = MessageService.addMessage(msgDef)
    result should include regex ("Message defintion file does not exist")
  }

  it should "produce  message def" in {

    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    val result = MessageService.addMessage(msgDef)
    result should include regex ("\"Result Description\" : \"Message Added Successfully")
  }

  it should "produce Higer version present when same or lower version added" in {

    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    var result = MessageService.addMessage(msgDef)
    result = MessageService.addMessage(msgDef)
    result should include regex ("\"Result Description\" : \"Error: com.ligadata.Exceptions.MsgCompilationFailedException: Higher active version of Message")
  }


  "delete message" should "delete the requested message definition from the metadata" in {
    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    MessageService.addMessage(msgDef)
    val result =MessageService.removeMessage("com.ligadata.kamanja.samples.messages.msg1.000000000001000000")
    result should include regex ("\"Result Description\" : \"Deleted Message Successfully")
  }

  it should " produce error if invalid message definition provided for deletion" in {
    val result =MessageService.removeMessage("system.invalid_msg_def.00000000000100001")
    result should include regex ("\"Result Description\" : \"Failed to Delete Message. Message Not found")
  }


  "get message " should " retrieve the requested message definition" in {
    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    MessageService.addMessage(msgDef)
    val result= MessageService.getMessage("com.ligadata.kamanja.samples.messages.msg1.000000000001000000")
    result should include regex ("\"Result Description\" : \"Successfully fetched message from cache")
  }

  "get message " should "fail to retrieve invalid(not present in Metadata) message definition " in {
    val result= MessageService.getMessage("system.invalid_msg_def.000000000001000000")
    result should include regex ("\"Result Description\" : \"Failed to fetch message from cache")
  }

  "update message " should "update a valid message definition " in {
    var msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    MessageService.addMessage(msgDef)
    msgDef= getClass.getResource("/Metadata/message/Message_Definition_HelloWorld_v2.json").getPath
    val result = MessageService.updateMessage(msgDef)
      result should include regex ("\"Result Description\" : \"Message Added Successfully")
  }

  it should "produce Higer version present when same or lower version added" in {
    var msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    MessageService.addMessage(msgDef)
    msgDef= getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    val result = MessageService.updateMessage(msgDef)
    result should include regex ("\"Status Code\" : -1")
  }

  "get all messages" should " return the list of messages in the metadata" in {
    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath
    MessageService.addMessage(msgDef)
    val result= MessageService.getAllMessages
    result should include regex ("\"Result Description\" : \"Successfully retrieved all the messages")
  }

  it should "produce no available messages when no " in {
    val result= MessageService.getAllMessages
    result should include regex ("\"Result Description\" : \"Sorry, No messages are available in the Metadata")
  }


  "is valid dir" should " validate if the directory is present" in {
    val msgDef = getClass.getResource("/Metadata/message").getPath
    val result=MessageService.IsValidDir(msgDef)
    assert(result==true)
  }

  it should " invalidate a wrong directory path" in {
    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath+"Invalid"
    val result=MessageService.IsValidDir(msgDef)
    assert(result==false)
  }

  "add Message by user input" should " upload the message definition to metadata based on the user input" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    MessageService.addMessage("") should include regex ("\"Result Description\" : \"Message Added Successfully")
  }

  "update message by user input " should "update the message definition to metadata based on the user input" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    MessageService.addMessage("")
    Console.setIn(new ByteArrayInputStream("2".getBytes))
    MessageService.updateMessage("") should include regex "Message Added Successfully"
  }

  "delete message by user input" should "delete the requested message definition from the metadata" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    MessageService.addMessage(getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath)
    MessageService.removeMessage("") should include regex ("\"Result Description\" : \"Deleted Message Successfully")
  }

  "get message by user input" should " retrieve the requested message definition" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    MessageService.addMessage(getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath)
    MessageService.getMessage("") should include regex ("\"Result Description\" : \"Successfully fetched message from cache")
  }

  "user input " should "get the user input and perform requested operation on it" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    val messages: Array[File] = new java.io.File(getClass.getResource("/Metadata/inputMessage").getPath).listFiles.filter(_.getName.endsWith(".json"))
    MessageService.getUserInputFromMainMenu(messages).size > 0
  }
}

