import com.ligadata.OutputAdapters.SmartFileProducer
import com.ligadata.InputOutputAdapterInfo.AdapterConfiguration
import com.ligadata.InputOutputAdapterInfo.CountersAdapter
import com.ligadata.InputOutputAdapterInfo.OutputAdapter
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator

object RunAdapter {
  def main(args: Array[String]): Unit = {
    Configurator.setRootLevel(Level.DEBUG);
    var adapter: OutputAdapter = null
    try {
    var inputConfig: AdapterConfiguration = new AdapterConfiguration
    var cntrAdapter: CountersAdapter = null
    
  
    inputConfig.Name = "SMFOutputTest"
    inputConfig.adapterSpecificCfg = "{\"Location\": \"file:/tmp/SMFOutputTest.txt\", \"CompressionString\": \"gz\"}"

    adapter = SmartFileProducer.CreateOutputAdapter(inputConfig, cntrAdapter)
    val messages = Array(
        "This is first test message for Smart File Output Adapter".getBytes,
        "This is second test message for Smart File Output Adapter".getBytes)
    val partKeys = Array(
        "key1".getBytes,
        "key2".getBytes)
    adapter.send(messages, partKeys)
    } finally {
      if(adapter != null) adapter.Shutdown
    }
  }
}