package com.ligadata.BasicCacheConcurrency



/**
  * Created by Saleh on 3/14/2016.
  */
object NodeEhCache {
  def main(args: Array[String]) {

    val aclass = Class.forName("com.ligadata.BasicCacheConcurrency.MemoryDataCacheImp").newInstance
    val node = aclass.asInstanceOf[DataCache]

    node.init("""{"name":"Node2","maxEntriesLocalHeap":"10000","maxEntriesLocalDisk":"1000","eternal":"false","diskSpoolBufferSizeMB":"20","timeToIdleSeconds":"300","timeToLiveSeconds":"600","memoryStoreEvictionPolicy":"LFU","transactionalMode":"off"}""")
    node.start()
    var map = new java.util.HashMap[String,String]
    map.put("1","1")

    node.put(map)

    System.out.println(node.get("1").toString)
    System.out.println(node.get("4").toString)

    node.shutdown()

  }
}




