import org.scalatest._
import Matchers._
import com.ligadata.kamanja.metadata.AdapterInfo
class AdapterInfoTestUnit extends FeatureSpec with GivenWhenThen {
  info("This an example to test AdapterInfo class")
  info("It consists of 14 functions")
  info("I want to be able to create an AdapterInfo object")
  info("So i can access the variables")
  info("And get all variables when I need it using functions")
  feature("AdapterInfo object") {
    scenario("Create an AdapterInfo object with initiate all variable") {
      val adapterInfoObj = new AdapterInfo()
      Given("Initiate variables")
      adapterInfoObj.name = "Ligadata"
      adapterInfoObj.typeString = "string"
      adapterInfoObj.dataFormat = "csv"
      adapterInfoObj.className = "kamanjaTest"
      adapterInfoObj.inputAdapterToValidate = "validate"
      adapterInfoObj.failedEventsAdapter = "\\03"
      adapterInfoObj.delimiterString1 = "\\04"
      adapterInfoObj.associatedMsg = "patientInfo.json"
      adapterInfoObj.jarName = "kamanjaTest-snapshot"
      adapterInfoObj.dependencyJars = Array("log4j.jar", "json.jar", "scala-2.10.jar")
      adapterInfoObj.adapterSpecificCfg = "adapter"
      adapterInfoObj.keyAndValueDelimiter = "|"
      //adapterInfoObj.fieldDelimiter = "\\02"
      adapterInfoObj.valueDelimiter = "\\a01"
      var stringVal = adapterInfoObj.Name
      Then("The name value should be set")
      stringVal should be("Ligadata")
      stringVal = adapterInfoObj.TypeString
      Then("The typeString value should be set")
      stringVal should be("string")
      stringVal = adapterInfoObj.DataFormat
      Then("The dataFormat value should be set")
      stringVal should be("csv")
      stringVal = adapterInfoObj.ClassName
      Then("The className value should be set")
      stringVal should be("kamanjaTest")
      stringVal = adapterInfoObj.InputAdapterToValidate
      Then("The inputAdapterToValidate value should be set")
      stringVal should be("validate")
      stringVal = adapterInfoObj.FailedEventsAdapter
      Then("The failedEventsAdapter value should be set")
      stringVal should be("\\03")
      stringVal = adapterInfoObj.DelimiterString1
      Then("The delimiterString1 value should be set")
      stringVal should be("\\04")
      stringVal = adapterInfoObj.FieldDelimiter
      Then("The fieldDelimiter value should be set")
      stringVal should be("\\04")
      Given("change fieldDelimiter")
      adapterInfoObj.fieldDelimiter = "\\02"
      stringVal = adapterInfoObj.FieldDelimiter
      Then("The fieldDelimiter value should be set")
      stringVal should be("\\02")
      stringVal = adapterInfoObj.DelimiterString1
      Then("The delimiterString1 value should be set")
      stringVal should be("\\02")
      stringVal = adapterInfoObj.AssociatedMessage
      Then("The associatedMsg value should be set")
      stringVal should be("patientInfo.json")
      stringVal = adapterInfoObj.JarName
      Then("The jarName value should be set")
      stringVal should be("kamanjaTest-snapshot")
      val stringArray = adapterInfoObj.dependencyJars
      Then("The dependencyJars first value should be set")
      stringArray(0) should be("log4j.jar")
      Then("The dependencyJars second value should be set")
      stringArray(1) should be("json.jar")
      Then("The dependencyJars third value should be set")
      stringArray(2) should be("scala-2.10.jar")
      stringVal = adapterInfoObj.AdapterSpecificCfg
      Then("The adapterSpecificCfg value should be set")
      stringVal should be("adapter")
      stringVal = adapterInfoObj.KeyAndValueDelimiter
      Then("The keyAndValueDelimiter value should be set")
      stringVal should be("|")
      //stringVal = adapterInfoObj.FieldDelimiter
      //Then("The fieldDelimiter value should be set")
      //stringVal should be("\\02")
      stringVal = adapterInfoObj.ValueDelimiter
      Then("The valueDelimiter value should be set")
      stringVal should be("\\a01")
    }
  }
}