package com.ligadata.adapters.jdbc;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

public class Md5ContainerSink extends AbstractJDBCSink {
	static Logger logger = Logger.getLogger(Md5ContainerSink.class);

	protected class Md5Partition {
		int key1;
		int key2;
		ArrayList<Long> hashValues;

		private Md5Partition(int key1, int key2) {
			this.key1 = key1;
			this.key2 = key2;
			this.hashValues = new ArrayList<>();
		}
	}
	
	private HashMap<String, Md5Partition> buffer = new HashMap<String, Md5Partition>();
	
	@Override
	public boolean addMessage(String message) {
		String[] fields = message.split(",");
		if(fields.length < 3) {
			logger.error("Incorrect message. Expecting atleast 3 fields. Message: " + message);
			return false;
		}
		
		try {
			String key = fields[0] + fields[1];
			Md5Partition md5 = buffer.get(key);
			if (md5 == null) {
				md5 = new Md5Partition(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]));
				md5.hashValues.add(Long.parseLong(fields[2]));
				buffer.put(key, md5);
			} else {
				md5.hashValues.add(Long.parseLong(fields[2]));
			}
		} catch (Exception e) {
			logger.error("Error: " + e.getMessage(), e);
			return false;
		}
		
		return true;
	}

	@Override
	public void processAll() throws Exception {
		for (String key : buffer.keySet()) {
			//Md5Partition md5 = buffer.get(key);
			// TODO Need to save to container
		}

	}

	@Override
	public void clearAll() {
		buffer.clear();
	}

}
