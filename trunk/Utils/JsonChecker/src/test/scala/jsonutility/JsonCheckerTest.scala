package com.ligadata.jsonutility

import org.scalatest._
import Matchers._
/**
  * Created by Yousef on 3/23/2016.
  */
class JsonCheckerTest extends FeatureSpec with GivenWhenThen {

  info("This an example to test JsonChecker class")
  info("I want to be able to create an JsonChecker object")
  info("So i can access the variables")
  info("And get all variables when I need it using functions")
  feature("JsonChecker object") {
    val JsonChecker = new JsonChecker()
    scenario("test if everything is ok") {
      Given("Initiate variables")
      val filePath = "C:\\Users\\Yousef\\Desktop\\BotnetDetection\\Jsonfile\\ConfigParameters.json"
      Given("Call readFile function")
     val fileContent = JsonChecker.ReadFile(filePath)
      Then("The fileContent value should be set")
      fileContent should not be (null)
      fileContent should not equal(0)

      Given("Call FileExist function")
      val fileExitFlag = JsonChecker.FileExist(filePath)
      Then("The file should be exists")
      fileExitFlag should be (true)

      Given("Call FindFileExtension function")
      val fileExtintionFlag = JsonChecker.FindFileExtension(filePath)
      Then("The file extension should be JSON")
      fileExtintionFlag should be (true)
    }

  scenario("test if  extension not json") {
    Given("Initiate variables")
    val filePath = "C:\\Users\\Yousef\\Desktop\\BotnetDetection\\Jsonfile\\ConfigParameters"

    Given("Call FileExist function")
    val fileExitFlag = JsonChecker.FileExist(filePath)
    Then("The file should be exists")
    fileExitFlag should be (false)

    Given("Call FindFileExtension function")
    val fileExtintionFlag = JsonChecker.FindFileExtension(filePath)
    Then("The file extension should be JSON")
    fileExtintionFlag should be (false)
  }

    scenario("test if no data in file") {
      Given("Initiate variables")
      val filePath = "C:\\Users\\Yousef\\Desktop\\BotnetDetection\\Jsonfile\\ConfigParameterstest.json"

      Given("Call readFile function")
      val fileContent = JsonChecker.ReadFile(filePath)
      Then("The fileContent value should be set")
      fileContent should  be (null)
      //fileContent should not equal(0)

      Given("Call FileExist function")
      val fileExitFlag = JsonChecker.FileExist(filePath)
      Then("The file should be exists")
      fileExitFlag should be (false)

      Given("Call FindFileExtension function")
      val fileExtintionFlag = JsonChecker.FindFileExtension(filePath)
      Then("The file extension should be JSON")
      fileExtintionFlag should be (false)
    }
  }
}
