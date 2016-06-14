import com.ligadata.cache.DataCache
import com.ligadata.cache.infinispan.EventCacheListener

/**
  * Created by Saleh on 6/12/2016.
  */
object InfiniNode {
  def main(args: Array[String]) {
    val aclass = Class.forName("com.ligadata.cache.infinispan.MemoryDataCacheImp").newInstance
    val node = aclass.asInstanceOf[DataCache]

    node.init("""{"name":"CacheCluster","jgroups.tcpping.initial_hosts":"192.168.1.122[7800],192.168.1.122[7801],192.168.1.122[7802]","jgroups.port":"7800","numOfKeyOwners":"2","CacheConfig":{"timeToIdleSeconds":"300000","timeToLiveSeconds":"300000","peerconfig":"jgroups_tcp.xml"}}""", new EventCacheListener)
    node.start()

    node.put("1","HI ALL".getBytes)

  }
}
