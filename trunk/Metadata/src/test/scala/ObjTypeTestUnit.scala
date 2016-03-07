import org.scalatest._
import Matchers._
import com.ligadata.kamanja.metadata.ObjType

class ObjTypeTestUnit extends FeatureSpec with GivenWhenThen {
  info("This an example to test ObjType class")
  info("It consists of 2 functions")
  info("I want to be able to create an ObjType object")
  info("So i can access the variables")
  info("And get all variables when I need it using functions")
  feature("ObjType object") {
    scenario("Create an ObjFormatType object with initiate all variables") {
      Given("Initiate variables")
      val stringVal = ObjType.asString(ObjType.tHashMap)
      Then("The value should be set")
      stringVal should be("HashMap")
      val objectVal = ObjType.fromString("HashMap")
      Then("The value should be set")
      objectVal should be(ObjType.tHashMap)
    }
  }
}