import com.ligadata.BasicCacheConcurrency.DataCache

/**
  * Created by Saleh on 3/23/2016.
  */
object Node2 {
  def main(args: Array[String]) {

    System.setProperty("java.net.preferIPv4Stack","true")

    val aclass = Class.forName("com.ligadata.BasicCacheConcurrency.MemoryDataCacheImp").newInstance
    val node = aclass.asInstanceOf[DataCache]

    node.init("""{"name":"CacheCluster","maxBytesLocalHeap":"20971520","maxBytesLocalDisk":"2097152","eternal":"false","diskSpoolBufferSizeMB":"20","timeToIdleSeconds":"300","timeToLiveSeconds":"300","memoryStoreEvictionPolicy":"LFU","transactionalMode":"off","class":"net.sf.ehcache.distribution.jgroups.JGroupsCacheManagerPeerProviderFactory","separator":"::","peerconfig":"channelName=EH_CACHE::file=jgroups_tcp.xml","replicatePuts":"true","replicateUpdates":"true","replicateUpdatesViaCopy":"false","replicateRemovals":"true","replicateAsynchronously":"true","bootstrapAsynchronously":"false"}""")
    node.start()

    System.out.println(node.get("1").toString)

  }
}
