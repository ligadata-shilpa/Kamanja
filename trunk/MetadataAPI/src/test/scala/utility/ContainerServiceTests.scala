package com.ligadata.MetadataAPI.utility.test
import org.scalatest._
import com.ligadata.kamanja.metadata._
import com.ligadata.MetadataAPI._
import com.ligadata.MetadataAPI.Utility.ContainerService

/**
 * Created by dhavalkolapkar on 3/21/16.
 */
class ContainerServiceTests extends FlatSpec with Matchers with MetadataBeforeAndAfterEach {

  "add container" should "add the valid contianer" in {
      ContainerService.addContainer(getClass.getResource("/Metadata/container/EnvCodes.json").getPath) should include regex ("Container Added Successfully")
  }

  "updateContainer" should "update a valid container" in {
    ContainerService.addContainer(getClass.getResource("/Metadata/container/EnvCodes.json").getPath)
    ContainerService.updateContainer(getClass.getResource("/Metadata/container/EnvCodesV2.json").getPath) should include regex ("Container Added Successfully")
  }

  "getContainer" should "get the requested container in the metadata" in {
    ContainerService.addContainer(getClass.getResource("/Metadata/container/EnvCodes.json").getPath)
    ContainerService.getContainer("system.context.000000000000000001") should include regex "Successfully fetched container from Cache"
  }

  "getAllContainers" should " get all the containers in the metadata" in {
    ContainerService.addContainer(getClass.getResource("/Metadata/container/EnvCodes.json").getPath)
    ContainerService.getAllContainers should include regex "Successfully retrieved all the messages"
  }

  "removeContainer" should "remove the requested container in the metadata" in {
    ContainerService.addContainer(getClass.getResource("/Metadata/container/EnvCodes.json").getPath)
    ContainerService.removeContainer("system.context.000000000000000001") should include regex "Deleted Container Successfully"
  }

  "is valid dir" should " validate if the directory is present" in {
    assert((ContainerService.IsValidDir(getClass.getResource("/Metadata/message").getPath))==true)
  }

  it should " invalidate a wrong directory path" in {
    assert((ContainerService.IsValidDir(getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath+"Invalid"))==false)
  }
}

