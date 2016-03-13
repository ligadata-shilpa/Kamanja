import java.io.Serializable

import net.sf.ehcache.{Element, Cache, CacheManager}

/**
  * Created by Saleh on 3/13/2016.
  */
object Test {
  def main(args: Array[String]) {
    var cm = CacheManager.newInstance("C:\\Users\\Saleh\\Documents\\GitHub\\Kamanja\\trunk\\BasicCacheCoherency\\src\\main\\resources\\ehcache.xml")
    var cache = cm.getCache("userCache")

    cache.put(new Element("1", "Jan"))
    cache.put(new Element("2", "Feb"))
    cache.put(new Element("3", "Mar"))
    cache.put(new Element("4", new Bean2("a","b")))


    System.out.println(cache.isKeyInCache("3"))
    System.out.println(cache.isKeyInCache("10"))

    val ele4: Element = cache.get("4")
    val b: Serializable = ele4.getValue

    System.out.println(">>>>>>>>>>>>>>> " + ele4.getObjectValue.toString)
    System.out.println(">>>>>>>>>>>>>>> " + (b.asInstanceOf[Bean2]).getB)
    System.out.println(">>>>>>>>>>>>>>> " + (b.asInstanceOf[Bean2]).getA)

//    cm.shutdown()
  }
}
