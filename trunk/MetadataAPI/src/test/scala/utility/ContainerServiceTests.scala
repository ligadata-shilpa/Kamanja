package com.ligadata.MetadataAPI.utility.test

import java.io.{File, ByteArrayInputStream}

import org.scalatest._
import com.ligadata.kamanja.metadata._
import com.ligadata.MetadataAPI._
import com.ligadata.MetadataAPI.Utility.ContainerService

/**
 * Created by dhavalkolapkar on 3/21/16.
 */
class ContainerServiceTests extends FlatSpec with Matchers with MetadataBeforeAndAfterEach {

  "add container" should "add the valid container" in {
      ContainerService.addContainer(getClass.getResource("/Metadata/container/EnvCodes.json").getPath) should include regex ("Container Added Successfully")
  }
  it should "add the container selected by the user" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    ContainerService.addContainer("") should include regex ("Container Added Successfully")
  }

  "updateContainer" should "update a valid container" in {
    ContainerService.addContainer(getClass.getResource("/Metadata/container/EnvCodes.json").getPath)
    ContainerService.updateContainer(getClass.getResource("/Metadata/container/EnvCodesV2.json").getPath) should include regex ("Container Added Successfully")
  }

 /* it should "update a container selected by the user" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    ContainerService.addContainer(getClass.getResource("/Metadata/container/EnvCodes.json").getPath)
    ContainerService.updateContainer("") should include regex ("Container Added Successfully")
  }*/

  "getContainer" should "get the requested container in the metadata" in {
    ContainerService.addContainer(getClass.getResource("/Metadata/container/EnvCodes.json").getPath)
    ContainerService.getContainer("system.context.000000000000000001") should include regex "Successfully fetched container from Cache"
  }

  it should "get the user requested container in the metadata" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    ContainerService.addContainer(getClass.getResource("/Metadata/container/EnvCodes.json").getPath)
    ContainerService.getContainer("") should include regex "Successfully fetched container from Cache"
  }

  "getAllContainers" should " get all the containers in the metadata" in {
    ContainerService.addContainer(getClass.getResource("/Metadata/container/EnvCodes.json").getPath)
    ContainerService.getAllContainers should include regex "Successfully retrieved all the messages"
  }

  "removeContainer" should "remove the requested container in the metadata" in {
    ContainerService.addContainer(getClass.getResource("/Metadata/container/EnvCodes.json").getPath)
    ContainerService.removeContainer("system.context.000000000000000001") should include regex "Deleted Container Successfully"
  }

  it should "remove the user requested container in the metadata" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    ContainerService.addContainer(getClass.getResource("/Metadata/container/EnvCodes.json").getPath)
    ContainerService.removeContainer("") should include regex "Deleted Container Successfully"
  }

  "is valid dir" should " validate if the directory is present" in {
    assert((ContainerService.IsValidDir(getClass.getResource("/Metadata/message").getPath))==true)
  }

  it should " invalidate a wrong directory path" in {
    assert((ContainerService.IsValidDir(getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath+"Invalid"))==false)
  }

  "user input " should "get the user input and perform requested operation on it" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    val messages: Array[File] = new java.io.File(getClass.getResource("/Metadata/inputMessage").getPath).listFiles.filter(_.getName.endsWith(".json"))
    ContainerService.getUserInputFromMainMenu(messages).size > 0
  }
}

