package com.ligadata.adapters.container;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.ligadata.KamanjaBase.DataDelimiters;
import com.ligadata.KamanjaBase.DelimitedData;
import com.ligadata.KamanjaBase.MessageContainerBase;
import com.ligadata.adapters.AdapterConfiguration;
import com.ligadata.adapters.BufferedMessageProcessor;
import com.ligadata.tools.SaveContainerDataComponent;

public class BufferedContainerSink implements BufferedMessageProcessor {
	static Logger logger = Logger.getLogger(BufferedContainerSink.class);

	private HashMap<String, ArrayList<String[]>> buffer = new HashMap<String, ArrayList<String[]>>();
	private String containerName = null;
	private String fieldDelimiter = ",";
	private String valueDelimiter = "~";
	private String kvDelimiter = ":";
	
	private SaveContainerDataComponent writer = null;
	
	@Override
	public void init(AdapterConfiguration config) throws Exception {
		writer = new SaveContainerDataComponent();
		String configFile = config.getProperty(AdapterConfiguration.METADATA_CONFIG_FILE);
		if(configFile == null)
			throw new Exception("Metadata config file not specified.");
		
		fieldDelimiter = config.getProperty(AdapterConfiguration.MESSAGE_FIELD_DELIMITER, ",");
		valueDelimiter = config.getProperty(AdapterConfiguration.MESSAGE_VALUE_DELIMITER, "~");
		kvDelimiter = config.getProperty(AdapterConfiguration.MESSAGE_KEYVALUE_DELIMITER, ":");

		containerName = config.getProperty(AdapterConfiguration.METADATA_CONTAINER_NAME);
		if(containerName == null)
			logger.warn("Container name not specified in the configuration file, will be expecting container name as the first field in the message.");

		writer.Init(configFile);
	}

	@Override
	public boolean addMessage(String message) {
		try {
			String key = containerName;
			String[] fields = message.split(fieldDelimiter);
			if(containerName == null) {
				key = fields[0];
				fields = Arrays.copyOfRange(fields, 1, fields.length);
			}
			
			ArrayList<String[]> records = buffer.get(key);
			if (records == null) {
				records = new ArrayList<String[]>();
				records.add(fields);
				buffer.put(key, records);
			} else {
				records.add(fields);
			}

		} catch (Exception e) {
			logger.error("Error: " + e.getMessage(), e);
			return false;
		}
		
		return true;
	}

	@Override
	public void processAll() throws Exception {
		DataDelimiters delimiters = new DataDelimiters();
		delimiters.keyAndValueDelimiter_$eq(kvDelimiter);
		delimiters.fieldDelimiter_$eq(fieldDelimiter);
		delimiters.valueDelimiter_$eq(valueDelimiter);
	
		for (String key : buffer.keySet()) {
			logger.debug("Container name is " + key);
			ArrayList<MessageContainerBase> data = new ArrayList<MessageContainerBase>();
			ArrayList<String[]> records = buffer.get(key);
			for(String[] tokens : records) {
				MessageContainerBase container = writer.GetMessageContainerBase(key);
						
				DelimitedData id = new DelimitedData("", delimiters);
				id.tokens_$eq(tokens);
				id.curPos_$eq(0);
			
				container.populate(id);
				data.add(container);
			}
			writer.SaveMessageContainerBase(key, data.toArray(new MessageContainerBase[0]), true, true);
		}
	}

	@Override
	public void clearAll() {
		buffer.clear();
	}

	@Override
	public void close() {
		writer.Shutdown();
	}

}
