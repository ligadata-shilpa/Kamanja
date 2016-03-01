import org.scalatest._
import com.ligadata.kamanja.metadata.ObjFormatType
import com.ligadata.kamanja.metadata.ObjType
import com.ligadata.kamanja.metadata.ObjTypeType
import com.ligadata.kamanja.metadata.UserPropertiesInfo
import Matchers._
import scala.io._
import com.ligadata.kamanja.metadata.UserPropertiesInfo

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
}