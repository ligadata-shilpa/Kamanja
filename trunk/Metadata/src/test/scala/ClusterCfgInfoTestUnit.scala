import org.scalatest._
import Matchers._
import java.util._
import scala.collection.mutable.HashMap
import com.ligadata.kamanja.metadata.ClusterCfgInfo
class ClusterCfgInfoTestUnit extends FeatureSpec with GivenWhenThen {
  info("This an example to take a file extention from enum using ObjFormatType")
  info("It take an object from enum ")
  info("And return 1 string")
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
}