import org.scalatest._
import Matchers._
import com.ligadata.kamanja.metadata.NodeInfo
class NodeInfoTestUnit extends FeatureSpec with GivenWhenThen {
  info("This an example to test NodeInfo class")
  info("It consists of 12 functions")
  info("I want to be able to create an NodeInfo object")
  info("So i can access the variables")
  info("And get all variables when I need it using functions")
  feature("NodeInfo object") {
    scenario("Create an NodeInfo object with initiate all variables") {
      val nodeInfoObj = new NodeInfo()
      Given("Initiate variables")
      nodeInfoObj.nodeId = "192.168.2.1"
      nodeInfoObj.nodePort = 8081
      nodeInfoObj.nodeIpAddr = "192.168.3.1"
      nodeInfoObj.jarPaths = Array("C://user", "D://", "E://")
      nodeInfoObj.scala_home = "/usr/opt/scala"
      nodeInfoObj.java_home = "/usr/opt/java"
      nodeInfoObj.classpath = "/usr/opt/lib"
      nodeInfoObj.clusterId = "192.168.2.1"
      nodeInfoObj.power = 1000
      nodeInfoObj.roles = Array("select", "delete")
      nodeInfoObj.description = "user privileges"
      var stringVal = nodeInfoObj.ClusterId
      Then("The clusterId value should be set")
      stringVal should be("192.168.2.1")
      stringVal = nodeInfoObj.NodeId
      Then("The nodeId value should be set")
      stringVal should be("192.168.2.1")
      var intVal = nodeInfoObj.NodePort
      Then("The nodePort value should be set")
      intVal should be(8081)
      stringVal = nodeInfoObj.NodeIpAddr
      Then("The nodeIpAddr value should be set")
      stringVal should be("192.168.3.1")
      var arrayVal = nodeInfoObj.JarPaths
      Then("The jarPaths value should be set")
      arrayVal(0) should be("C://user")
      arrayVal(1) should be("D://")
      arrayVal(2) should be("E://")
      stringVal = nodeInfoObj.Scala_home
      Then("The scala_Home value should be set")
      stringVal should be("/usr/opt/scala")
      stringVal = nodeInfoObj.Java_home
      Then("The java_Home value should be set")
      stringVal should be("/usr/opt/java")
      stringVal = nodeInfoObj.Description
      Then("The description value should be set")
      stringVal should be("user privileges")
      intVal = nodeInfoObj.Power
      Then("The power value should be set")
      intVal should be(1000)
      arrayVal = nodeInfoObj.Roles
      Then("The roles value should be set")
      arrayVal(0) should be("select")
      arrayVal(1) should be("delete")
    }
  }
}