package com.ligadata.adapters;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class AdapterConfiguration {
	private Properties properties;

	public AdapterConfiguration() throws IOException {
		this("config.properties");
	}

	public AdapterConfiguration(String configFileName) throws IOException {
		File configFile = new File(configFileName);		 
	    FileReader reader = null;
	    
		try {
		    reader = new FileReader(configFile);
		    properties = new Properties();
		    properties.load(reader);
		 
		} finally {
			if(reader != null)
				try { reader.close(); } catch (Exception e){}
		}	
	}

	public Properties getProperties() {
		return properties;
	}	
	
	public String getProperty(String name) {
		return properties.getProperty(name);
	}

	public String getProperty(String name, String defaultValue) {
		return properties.getProperty(name, defaultValue);
	}
}
