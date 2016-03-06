import org.scalatest._
import com.ligadata.kamanja.metadata.ObjFormatType
import com.ligadata.kamanja.metadata.ObjType
import com.ligadata.kamanja.metadata.ObjTypeType
import com.ligadata.kamanja.metadata.UserPropertiesInfo
import Matchers._
import scala.io._
import java.util._
import com.ligadata.kamanja.metadata.UserPropertiesInfo
import com.ligadata.kamanja.metadata.AdapterInfo
import com.ligadata.kamanja.metadata.ClusterCfgInfo
import com.ligadata.kamanja.metadata.ClusterInfo
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.HashMap
import com.ligadata.kamanja.metadata.ClusterInfo
import com.ligadata.kamanja.metadata.NodeInfo

class mdelemsTestUnit extends FeatureSpec with GivenWhenThen {
  info("This an example to take a file extention from enum using ObjFormatType")
  info("It take an object from enum ")
  info("And return 1 string")

  feature("test ObjFormatType") {
    scenario("Check if asString function return correct value") {
      Given("a return value is stored")
      val stringVal = ObjFormatType.asString(ObjFormatType.fCSV)
      Then("The value should be set")
      stringVal should be("CSV")
    }
    scenario("Check if fromString function return correct value") {
      Given("a return value is stored")
      val stringVal = ObjFormatType.fromString("CSV")
      Then("The value should be Set")
      stringVal should be(ObjFormatType.fCSV)
    }
  }

  feature("test ObjType") {
    scenario("Check if asString function return correct value") {
      Given("a return value is stored")
      val stringVal = ObjType.asString(ObjType.tHashMap)
      Then("The value should be set")
      stringVal should be("HashMap")
    }
    scenario("Check if fromString function return correct value") {
      Given("a return value is stored")
      val stringVal = ObjType.fromString("HashMap")
      Then("The value should be set")
      stringVal should be(ObjType.tHashMap)
    }
  }

  feature("test ObjTypeType") {
    scenario("Check if asString function return correct value") {
      Given("a return value is stored")
      val stringVal = ObjTypeType.asString(ObjTypeType.tScalar)
      Then("The value should be set")
      stringVal should be(ObjTypeType.tScalar.toString())
    }
  }

  feature("test UserPropertiesInfo") {
    scenario("Check if clusterId function return correct value") {
      val userPropertiesInfoObj = new UserPropertiesInfo()
      Given("a return value is stored")
      userPropertiesInfoObj.clusterId = "192.168.2.1"
      val stringVal = userPropertiesInfoObj.ClusterId
      Then("The value should be set")
      stringVal should be("192.168.2.1")
    }
  }

  feature("test AdapterInfo") {
    scenario("Check if Class functions return correct value") {
      val adapterInfoObj = new AdapterInfo()
      Given("modify variables value")
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

  feature("test ClusterCfgInfo") {
    scenario("Check if Class functions work correctly") {
      Given("modify variables value")
      val clusterCfgInfoObj = new ClusterCfgInfo()
      val hashmapcfg = new HashMap[String, String]()
      hashmapcfg += ("one" -> "kamanja1", "two" -> "kamanja2")
      val hashmapUSerConfig = new HashMap[String, String]()
      hashmapUSerConfig += ("one" -> "node1", "two" -> "node2")
      clusterCfgInfoObj.clusterId = "192.168.1.1"
      clusterCfgInfoObj.usrConfigs = hashmapUSerConfig
      clusterCfgInfoObj.cfgMap = hashmapcfg
      //clusterCfgInfoObj.modifiedTime = new Date("2016-02-01")
      //clusterCfgInfoObj.createdTime = new Date("2016-01-01")
      val stringVal = clusterCfgInfoObj.ClusterId
      Then("The clusterId value should be set")
      stringVal should be("192.168.1.1")
      //var dateVal = clusterCfgInfoObj.ModifiedTime
      //Then("The modifiedTime value should be set")
      //dateVal should be("2016-02-01")
      //dateVal = clusterCfgInfoObj.CreatedTime
      //Then("The createdTime value should be set")
      //dateVal should be("2016-01-01")
      val hashmapval = clusterCfgInfoObj.CfgMap
      Then("The userConfigs should be set")
      hashmapval("one") should be("kamanja1")
      hashmapval("two") should be("kamanja2")
      val hashmapva2 = clusterCfgInfoObj.getUsrConfigs
      Then("The cfgMap should be set")
      hashmapva2("one") should be("node1")
      hashmapva2("two") should be("node2")
    }
  }

  feature("test ClusterInfo") {
    scenario("Check if Class functions return correct value") {
      val clusterInfoObj = new ClusterInfo()
      Given("a return value is stored")
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

  feature("test NodeInfo") {
    scenario("Check if Class function return correct value") {
      val nodeInfoObj = new NodeInfo()
      Given("a return value is stored")
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