
import org.scalatest._
import com.ligadata.AdaptersConfiguration.KafkaQueueAdapterConfiguration
import com.ligadata.InputOutputAdapterInfo.AdapterConfiguration

class CheckConfig extends FlatSpec with BeforeAndAfter with Matchers {

  var adpterConfig: AdapterConfiguration = null
  var kafkaConsumer: KafkaQueueAdapterConfiguration = null

  before {
    adpterConfig = new AdapterConfiguration()
    adpterConfig.adapterSpecificCfg = """{"HostList":"localhost:9092","TopicName":"testin_1"}"""
    kafkaConsumer = new KafkaQueueAdapterConfiguration
  }

  "GetAdapterConfig" should "return config after parsing json configration" in {

    val input = adpterConfig
    val expected = kafkaConsumer
    expected.topic = "testin_1"
    expected.hosts = Array[String]("localhost:9092")

    val actual = KafkaQueueAdapterConfiguration.GetAdapterConfig(input)
    assertResult(expected.hosts(0))(actual.hosts(0))
    assertResult(expected.topic)(actual.topic)
  }

}