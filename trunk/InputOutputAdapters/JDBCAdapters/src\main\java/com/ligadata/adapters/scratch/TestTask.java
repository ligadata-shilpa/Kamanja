package com.ligadata.adapters.scratch;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.ligadata.adapters.utility.WrappedScheduleExecutor;

public class TestTask implements Runnable {
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("Sleeping ..."+sdf.format(new Date()));
        try {
                Thread.sleep(100);
        } catch (InterruptedException e) {
                e.printStackTrace();
        }
        
        //Dangerous Code breaks the SimgleThreadSchdeuledExecutor
        System.out.println("Throwing ... "+sdf.format(new Date()));
        throw new RuntimeException("Bad Exception!");
	}
	
	public static void main(String[] args) {
		 new WrappedScheduleExecutor(1).scheduleAtFixedRate(new TestTask(), 1, 1, TimeUnit.SECONDS);
	}

}
