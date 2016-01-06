package com.ligadata.adapters.pipeline;

import org.json.simple.JSONObject;

public interface IPipeline extends Runnable{
	void create(JSONObject configs);
	void run();
	void close();
}
