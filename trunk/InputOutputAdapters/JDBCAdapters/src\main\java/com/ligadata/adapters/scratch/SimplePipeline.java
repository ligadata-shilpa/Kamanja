package com.ligadata.adapters.scratch;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.simple.JSONObject;

import com.ligadata.adapters.pipeline.IPipeline;

public class SimplePipeline implements IPipeline {
	JSONObject config;
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

	@Override
	public void create(JSONObject configs) {
		// TODO Auto-generated method stub
		config = configs;
		System.out.println("Created at "+sdf.format(new Date()));
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("Started Running at "+sdf.format(new Date()));
		throw new RuntimeException("Test Exception");
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		System.out.println("Closed at "+sdf.format(new Date()));
	}

}
