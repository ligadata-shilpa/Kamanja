package com.ligadata.BasicCacheConcurrency



/**
  * Created by Saleh on 3/14/2016.
  */
object NodeEhCache {
  def main(args: Array[String]) {

    val aclass = Class.forName("com.ligadata.BasicCacheConcurrency.MemoryDataCacheImp").newInstance
    val node = aclass.asInstanceOf[DataCache]

    node.init("""{"name":"Node1","maxBytesLocalHeap":"1000","maxBytesLocalDisk":"100","eternal":"false","diskSpoolBufferSizeMB":"20","timeToIdleSeconds":"3000","timeToLiveSeconds":"300000","memoryStoreEvictionPolicy":"LFU","transactionalMode":"off","class":"net.sf.ehcache.distribution.jgroups.JGroupsCacheManagerPeerProviderFactory","separator":"::","peerconfig":"channelName=EH_CACHE::file=jgroups.xml","replicatePuts":"true","replicateUpdates":"true","replicateUpdatesViaCopy":"false","replicateRemovals":"true","replicateAsynchronously":"true","bootstrapAsynchronously":"false"}""")
    node.start()
    var map = new java.util.HashMap[String,String]


//    for(i<-1 until 10){
//      //System.out.println(i)
//      node.put(""+i,""+i)
//    }

    System.out.println(node.get("1").toString)
    System.out.println(node.get("2").toString)
    System.out.println(node.get("4").toString)
    System.out.println(node.get("7").toString)
    System.out.println(node.get("10").toString)


//    node.shutdown()


  }
}




