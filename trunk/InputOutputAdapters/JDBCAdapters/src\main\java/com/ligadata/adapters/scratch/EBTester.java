package com.ligadata.adapters.scratch;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.joda.time.DateTime;

import com.google.common.eventbus.EventBus;
import com.ligadata.adapters.utility.SimpleEventListener;

public class EBTester {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ExecutorService exec = Executors.newFixedThreadPool(16);
		//AsyncEventBus eb = new AsyncEventBus("my eb1", exec);
		EventBus eb = new EventBus("my eb1");
		eb.register(new SimpleEventListener());
		System.out.println("Emitting a stream of events");
		DateTime dt = new DateTime();
		for(int j=0; j<100000; j++){
			eb.post(":: Event "+j+" posted at "+new Date()+" ::");
		}
		DateTime dt1 = new DateTime();
		long diffInMillis = dt1.getMillis() - dt.getMillis();
		System.out.println("Time taken - "+diffInMillis);
		exec.shutdown();
	}
}

/*
class SimpleEventListener {
	@Subscribe @AllowConcurrentEvents
	public void task(String s){
		System.out.println("Recevied "+s+" @ "+new Date());
	}
}
*/
