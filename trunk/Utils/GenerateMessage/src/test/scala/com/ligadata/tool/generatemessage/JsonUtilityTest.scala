package com.ligadata.tool.generatemessage

import org.json4s.native.JsonMethods._
import org.scalatest._
import Matchers._

/**
  * Created by Yousef on 5/18/2016.
  */
class JsonUtilityTest extends FeatureSpec with GivenWhenThen {
  private def getResourceFullPath(resourcePath : String): String ={
    val os = System.getProperty("os.name")
    val isWidnows = os.toLowerCase.contains("windows")
    val path = getClass.getResource(resourcePath).getPath
    val finalPath = if(isWidnows) path.substring(1) else path
    finalPath
  }

  info("This an example to test JsonUtility class")
  info("I want to be able to create an object")
  info("So i can access the variables")
  info("And get all variables when I need it using functions")

  feature("JsonUtility object") {
    scenario("Unit Tests for all JsonUtility functions") {
      Given("Initiate variables")
      val jsonObj: JsonUtility = new JsonUtility()
      val jsonUtilityBean: JsonUtility = new JsonUtility()
      val fileBean: FileUtility = new FileUtility()
      val filePath = getResourceFullPath("/configFile.json")
      val configFileContent = fileBean.ReadFile(filePath)
      val parsedFile = fileBean.ParseFile(configFileContent)
      val extractedFile = fileBean.extractInfo(parsedFile)
      var feildsString = Map[String, String]()
      val inputFile = getResourceFullPath("/inputFile.txt")
      val inputFileContent = fileBean.ReadFile(inputFile)
      val fileSize = fileBean.Countlines(inputFile)
      val configBeanObj = fileBean.createConfigBeanObj(extractedFile)
      val headerString = fileBean.ReadHeaderFile(inputFile, 0)
      val headerFields = fileBean.SplitFile(headerString, configBeanObj.delimiter)
      val dataTypeObj: DataTypeUtility = new DataTypeUtility()
      for(itemIndex <- 0 to headerFields.length-1) {
        var feildType1 = ""
        if (fileSize >= 2) {
          val fieldLines = fileBean.ReadHeaderFile(inputFile, 1)
          val linesfeild = fileBean.SplitFile(fieldLines, configBeanObj.delimiter)
          feildType1 = dataTypeObj.FindFeildType(linesfeild(itemIndex))
          feildsString = feildsString + (headerFields(itemIndex) -> feildType1)
        } else {
          feildsString = feildsString + (headerFields(itemIndex) -> "String")
        }
      }
      Given("Test CreateMainJsonString function")

      When("pass a map with data")
      var json = jsonObj.CreateMainJsonString(feildsString, configBeanObj)
      Then("The isDigitFlag value should be false")
      //isDigitFlag should be (false)
      println(pretty(render(json)))

      configBeanObj.partitionKey_=(true)
      val jsonPatitionKey = jsonObj.CreateJsonString("PartitionKey", configBeanObj)
      json = json merge jsonPatitionKey
      println("=======================================================")
      println(pretty(render(json)))
    }
  }
}
