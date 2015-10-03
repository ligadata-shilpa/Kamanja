package com.ligadata.adapters;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class AdapterConfiguration {
	public static final String SCHEMA_FILE = "schema.file";
	public static final String HDFS_URI = "hdfs.uri";
	public static final String FILE_PREFIX = "file.prefix";
	public static final String SYNC_MESSAGE_COUNT = "sync.messages.count";
	public static final String SYNC_INTERVAL_SECONDS = "sync.interval.seconds";
	public static final String KAFKA_TOPIC = "kafka.topic";
	public static final String COUNSUMER_THREADS = "consumer.threads";
	public static final String KAFKA_GROUP_ID = "kafka.group.id";
	public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
	public static final String ZOOKEEPER_SESSION_TIMEOUT = "zookeeper.session.timeout.ms";
	public static final String ZOOKEEPER_SYNC_TIME = "zookeeper.sync.time.ms";
	public static final String FILE_COMPRESSION = "file.compression";
	public static final String PARTITION_STRATEGY = "file.partition.strategy";
	public static final String INPUT_DATE_FORMAT = "input.date.format";
	public static final String MESSAGE_PROCESSOR = "adapter.message.processor";
	public static final String JDBC_DRIVER = "jdbc.driver";
	public static final String JDBC_URL = "jdbc.url";
	public static final String JDBC_USER = "jdbc.user";
	public static final String JDBC_PASSWORD = "jdbc.password";
	public static final String JDBC_INSERT_STATEMENT = "jdbc.insert.statement";
	public static final String JDBC_UPDATE_STATEMENT = "jdbc.update.statement";

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
