import com.ligadata.cache.DataCache

/**
  * Created by Saleh on 6/12/2016.
  */
object InfiniNode {
  def main(args: Array[String]) {
    val aclass = Class.forName("com.ligadata.cache.infinispan.MemoryDataCacheImp").newInstance
    val node = aclass.asInstanceOf[DataCache]

    node.init("""{"name":"CacheCluster","jgroups.tcpping.initial_hosts":"192.168.1.5[7800],192.168.1.5[7801]","jgroups.port":"7800","CacheConfig":{"timeToIdleSeconds":"30000","timeToLiveSeconds":"30000","peerconfig":"jgroups_tcp.xml"}}""", null)
    node.start()

    node.put("1","HI ALL".getBytes)

  }
}
