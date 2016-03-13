import java.io.Serializable;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

public class HelloEhCache{

	public static void main(String[] args) {

		//1. Create a cache manager
		CacheManager cm = CacheManager.getInstance();

		//cm.addCache("cache1");

		//2. Get a cache called "cache1", declared in ehcache.xml
//		Cache cache = cm.getCache("cache1");
		Cache cache = cm.getCache("userCache");

//		//3. Put few elements in cache
		cache.put(new Element("1","Jan"));
		cache.put(new Element("2","Feb"));
		cache.put(new Element("3","Mar"));
		cache.put(new Element("4",new Bean()));

		//4. Get element from cache
		Element ele = cache.get("2");

		//5. Print out the element
		String output = (ele == null ? null : ele.getObjectValue().toString());
		System.out.println(output);

		//6. Is key in cache?
		System.out.println(cache.isKeyInCache("3"));
		System.out.println(cache.isKeyInCache("10"));


		Element ele4 = cache.get("4");
		Serializable  b = ele4.getValue();

		System.out.println(">>>>>>>>>>>>>>> " + ele4.getObjectValue().toString());
		System.out.println(">>>>>>>>>>>>>>> " + ((Bean)b).getB());
		System.out.println(">>>>>>>>>>>>>>> " + ((Bean)b).getA());



		Element e = cache.get("4");
		Bean b1 = (e == null ? null : (Bean)e.getValue());
		System.out.println(">>>>>>>>>>>>>>> " + b1.getA());
		System.out.println(">>>>>>>>>>>>>>> " + b1.getB());
//
//
//		//7. shut down the cache manager
//		cm.shutdown();
	}

}