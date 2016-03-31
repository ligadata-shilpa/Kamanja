import com.ligadata.cache.DataCache

/**
  * Created by Saleh on 3/23/2016.
  */
object TestMaxSize {
  def main(args: Array[String]) {

    val aclass = Class.forName("com.ligadata.BasicCacheConcurrency.MemoryDataCacheImp").newInstance
    val node = aclass.asInstanceOf[DataCache]

    node.init("""{"name":"CacheCluster","maxBytesLocalHeap":"1048576","eternal":"false","diskSpoolBufferSizeMB":"20","timeToIdleSeconds":"300","timeToLiveSeconds":"300","memoryStoreEvictionPolicy":"LFU","transactionalMode":"off","class":"net.sf.ehcache.distribution.jgroups.JGroupsCacheManagerPeerProviderFactory","separator":"::","peerconfig":"channelName=EH_CACHE::file=jgroups_tcp.xml","replicatePuts":"true","replicateUpdates":"true","replicateUpdatesViaCopy":"false","replicateRemovals":"true","replicateAsynchronously":"true","bootstrapAsynchronously":"false","jgroups.tcpping.initial_hosts":"192.168.1.129[7800],192.168.1.11[7800]","jgroups.port":"7800"}""")
    node.start()

    System.out.println("====================")
    for(i<-1 until 1000){
      node.put(i.toString,"this is a memory test"+i)
    }
    System.out.println("====================")

    System.out.println("====================")
    for(i<-1 until 1000) {
      System.out.println(node.get(i.toString).toString)
    }
    System.out.println("====================")

    System.out.println("====================")
    for(i<-1 until 1000) {
      System.out.println(node.get(i.toString).toString)
    }
    System.out.println("====================")

  }
}
