import org.scalatest._
import Matchers._
import com.ligadata.kamanja.metadata.UserPropertiesInfo
class UserPropertiesInfoTestUnit extends FeatureSpec with GivenWhenThen {
  info("This an example to test UserPropertiesInfo class")
  info("It consists of 1 function")
  info("I want to be able to create an UserPropertiesInfo object")
  info("So i can access the variables")
  info("And get all variables when I need it using functions")
  feature("UserPropertiesInfo object") {
    scenario("Create an UserPropertiesInfo object with initiate all variables") {
      val userPropertiesInfoObj = new UserPropertiesInfo()
      Given("Initiate variables")
      userPropertiesInfoObj.clusterId = "192.168.2.1"
      val stringVal = userPropertiesInfoObj.ClusterId
      Then("The value should be set")
      stringVal should be("192.168.2.1")
    }
  }
}