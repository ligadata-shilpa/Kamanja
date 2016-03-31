import com.ligadata.cache.DataCache

/**
  * Created by Saleh on 3/23/2016.
  */
object Node1 {
  def main(args: Array[String]) {


    val aclass = Class.forName("com.ligadata.cache.MemoryDataCacheImp").newInstance
    val node = aclass.asInstanceOf[DataCache]

    node.init("""{"name":"CacheCluster","maxBytesLocalHeap":"20971520","eternal":"false","diskSpoolBufferSizeMB":"20","timeToIdleSeconds":"3000","timeToLiveSeconds":"3000","memoryStoreEvictionPolicy":"LFU","transactionalMode":"off","class":"net.sf.ehcache.distribution.jgroups.JGroupsCacheManagerPeerProviderFactory","separator":"::","peerconfig":"channelName=EH_CACHE::file=jgroups_tcp.xml","replicatePuts":"true","replicateUpdates":"true","replicateUpdatesViaCopy":"false","replicateRemovals":"true","replicateAsynchronously":"true","bootstrapAsynchronously":"false","enableListener":"true","jgroups.tcpping.initial_hosts":"192.168.1.129[7800],192.168.1.129[7800]","jgroups.port":"7800"}""")
    node.start()

    node.put("1","HI ALL".getBytes)

  }
}
