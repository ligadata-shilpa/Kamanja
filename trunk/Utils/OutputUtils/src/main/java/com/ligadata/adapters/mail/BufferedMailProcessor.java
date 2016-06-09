package com.ligadata.adapters.mail;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.ligadata.adapters.AdapterConfiguration;
import com.ligadata.adapters.BufferedMessageProcessor;
import com.ligadata.adapters.mail.pojo.Constants;
import com.ligadata.adapters.mail.pojo.SimpleMailBean;

public class BufferedMailProcessor implements BufferedMessageProcessor {
	static Logger logger = Logger.getLogger(BufferedMailProcessor.class);
	private ArrayList<JSONObject> buffer;
	private AdapterConfiguration conf;

	@Override
	public void init(AdapterConfiguration config) throws Exception {

		// Create a safe collection for access by multiple threads
		buffer = new ArrayList<JSONObject>();
		conf = config;
	}

	@Override
	public boolean addMessage(String message) {
		try {
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = (JSONObject) jsonParser.parse(message);

			buffer.add(jsonObject);
		} catch (Exception e) {
			logger.error(Constants.JSON_PARSE_ERROR + e.getMessage(), e);
			return false;
		}
		return true;
	}

	@Override
	public void processAll() throws Exception {
		MailProcessor mailProcessor = MailProcessor.getInstance(conf);

		for (JSONObject item : buffer) {
			if (item.get("emailNotify").toString().equalsIgnoreCase("Y")) {
				SimpleMailBean bean = new SimpleMailBean();
				bean.setConf(conf);
				bean.setFillers(item);
				bean.setTemplateDirectory(conf.getProperty(AdapterConfiguration.TEMPLATE_DIRECTORY));
				bean.populateData();

				mailProcessor.processBean(bean);
			}
		}
	}

	@Override
	public void clearAll() {
		buffer.clear();
	}

	@Override
	public void close() {
		// Close all resources here

	}

}