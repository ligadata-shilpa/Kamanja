
import org.scalatest._
import com.ligadata.kamanja.metadata.Utils
import Matchers._
import scala.io._

class UtilsTestUnit extends FeatureSpec with BeforeAndAfter with BeforeAndAfterAll with GivenWhenThen {

  info("This an example to parse a token using Utils")
  info("It take namespace.name.version as input")
  info("And return 3 strings (namespace, name, version)")
  feature("Token object") {
    scenario("Use namespace.name.version to parse") {
      Given("a token object is created")
      val newToken = Utils.parseNameToken("Kamanja.employee.five")
      Then("the namespace should be set")
      val namespace = newToken._1
      namespace should be("Kamanja")
      Then("the name should be set")
      val name = newToken._2
      name should be("employee")
      Then("the version should be set")
      val version = newToken._3
      version should be("five")
    }
  }
  scenario("Use namespace.name to parse") {
    val newToken = Utils.parseNameTokenNoVersion("Kamanja.employee")
    Then("the namespace should be set")
    val namespace = newToken._1
    namespace should be("Kamanja")
    Then("the name should be set")
    val name = newToken._2
    name should be("employee")
  }
  scenario("Use namespace and name to produce namespace.name") {
    val fullName = Utils.makeFullName("Kamanja", "employee")
    Then("the namespace should be")
    fullName should be("Kamanja.employee")
  }
}