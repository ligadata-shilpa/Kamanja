import org.scalatest._
import Matchers._
import com.ligadata.kamanja.metadata.ObjTypeType
class ObjTypeTypeTestUnit extends FeatureSpec with GivenWhenThen {
  info("This an example to test ObjTypeType class")
  info("It consists of 1 function")
  info("I want to be able to create an ObjTypeType object")
  info("So i can access the variables")
  info("And get all variables when I need it using functions")
    feature("ObjTypeType object") {
    scenario("Create an ObjTypeType object with initiate all variables") {
      Given("Initiate variables")
      val stringVal = ObjTypeType.asString(ObjTypeType.tScalar)
      Then("The value should be set")
      stringVal should be(ObjTypeType.tScalar.toString())
    }
  }
}