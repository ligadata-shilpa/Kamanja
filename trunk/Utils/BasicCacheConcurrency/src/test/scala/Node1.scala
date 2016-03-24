import com.ligadata.BasicCacheConcurrency.DataCache

/**
  * Created by Saleh on 3/23/2016.
  */
object Node1 {
  def main(args: Array[String]) {


    val aclass = Class.forName("com.ligadata.BasicCacheConcurrency.MemoryDataCacheImp").newInstance
    val node = aclass.asInstanceOf[DataCache]

    node.init("""{"name":"CacheCluster","maxBytesLocalHeap":"20971520","eternal":"false","timeToIdleSeconds":"300","timeToLiveSeconds":"300","memoryStoreEvictionPolicy":"LFU","transactionalMode":"off","class":"net.sf.ehcache.distribution.jgroups.JGroupsCacheManagerPeerProviderFactory","separator":"::","peerconfig":"channelName=EH_CACHE::file=jgroups_tcp.xml","replicatePuts":"true","replicateUpdates":"true","replicateUpdatesViaCopy":"false","replicateRemovals":"true","replicateAsynchronously":"true","bootstrapAsynchronously":"false"}""")
    node.start()

    node.put("1","HI ALL")

  }
}
