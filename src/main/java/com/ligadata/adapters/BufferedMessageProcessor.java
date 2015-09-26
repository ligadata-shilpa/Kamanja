package com.ligadata.adapters;

public interface BufferedMessageProcessor {
	public void init(AdapterConfiguration config) throws Exception;

	public void addMessage(String message);
	
	public void processAll() throws Exception;

	public void clearAll();
}
