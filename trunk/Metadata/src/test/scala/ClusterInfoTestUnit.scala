import org.scalatest._
import Matchers._
import com.ligadata.kamanja.metadata.ClusterInfo
class ClusterInfoTestUnit extends FeatureSpec with GivenWhenThen {
  info("This an example to test ClusterInfo class")
  info("It consists of 3 functions")
  info("I want to be able to create an ClusterInfo object")
  info("So i can access the variables")
  info("And get all variables when I need it using functions")
  feature("ClusterInfo object") {
    scenario("Create an AdapterInfo object with initiate all variables") {
      val clusterInfoObj = new ClusterInfo()
      Given("Initiate variables")
      clusterInfoObj.clusterId = "192.168.2.1"
      clusterInfoObj.description = "clster privileges"
      clusterInfoObj.privileges = "superuser"
      var stringVal = clusterInfoObj.ClusterId
      Then("The clusterId value should be set")
      stringVal should be("192.168.2.1")
      stringVal = clusterInfoObj.Description
      Then("The description value should be set")
      stringVal should be("clster privileges")
      stringVal = clusterInfoObj.Privileges
      Then("The privileges value should be set")
      stringVal should be("superuser")
    }
  }
}