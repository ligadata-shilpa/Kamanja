import org.scalatest._
import Matchers._
import com.ligadata.kamanja.metadata.ObjFormatType

class ObjFormatTypeTestUnit extends FeatureSpec with GivenWhenThen {
  info("This an example to test ObjFormatType class")
  info("It consists of 2 functions")
  info("I want to be able to create an ObjFormatType object")
  info("So i can access the variables")
  info("And get all variables when I need it using functions")

  feature("ObjFormatType object") {
    scenario("Create an ObjFormatType object with initiate all variables") {
      Given("Initiate variables")
      val stringVal = ObjFormatType.asString(ObjFormatType.fCSV)
      Then("The value should be set")
      stringVal should be("CSV")
      val objectVal = ObjFormatType.fromString("CSV")
      Then("The value should be Set")
      objectVal should be(ObjFormatType.fCSV)
    }
  }
}