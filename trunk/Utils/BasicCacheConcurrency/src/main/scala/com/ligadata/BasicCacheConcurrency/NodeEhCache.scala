package com.ligadata.BasicCacheConcurrency



/**
  * Created by Saleh on 3/14/2016.
  */
object NodeEhCache {
  def main(args: Array[String]) {

    val aclass = Class.forName("com.ligadata.BasicCacheConcurrency.MemoryDataCacheImp").newInstance
    val node = aclass.asInstanceOf[DataCache]

    node.init("""{"xmlPath":"C:\\Users\\Saleh\\Documents\\GitHub\\Kamanja\\trunk\\Utils\\BasicCacheConcurrency\\src\\main\\resources\\ehcache.xml","cacheName":"Node"}""")
    node.start()
    var map = new java.util.HashMap[String,String]
    map.put("1","1")

    node.put(map)

    System.out.println(node.get("1").toString)
    System.out.println(node.get("4").toString)

    node.shutdown()

  }
}




