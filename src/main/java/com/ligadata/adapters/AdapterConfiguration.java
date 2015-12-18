package com.ligadata.adapters;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;
import org.apache.log4j.Logger;

public class AdapterConfiguration {
	static Logger logger = Logger.getLogger(AdapterConfiguration.class); 
	
	public static final String SCHEMA_FILE = "schema.file";
	public static final String HDFS_URI = "hdfs.uri";
	public static final String HDFS_KERBEROS_KEYTABFILE = "hdfs.kerberos.keytabfile";
	public static final String HDFS_KERBEROS_PRINCIPAL = "hdfs.kerberos.principal";
	public static final String HDFS_RESOURCE_FILE = "hdfs.resource.file";
	public static final String FILE_PREFIX = "file.prefix";
	public static final String SYNC_MESSAGE_COUNT = "sync.messages.count";
	public static final String SYNC_INTERVAL_SECONDS = "sync.interval.seconds";
	public static final String KAFKA_TOPIC = "kafka.topic";
	public static final String COUNSUMER_THREADS = "consumer.threads";
	public static final String KAFKA_GROUP_ID = "kafka.group.id";
	public static final String KAFKA_OFFSETS_STORAGE = "kafka.offsets.storage";
	public static final String KAFKA_AUTO_OFFSET_RESET = "kafka.auto.offset.reset";
	public static final String KAFKA_PROPERTY_PREFIX = "kafka.";
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
	public static final String METADATA_CONFIG_FILE = "metadata.config.file";
	public static final String METADATA_CONTAINER_NAME = "metadata.container.name";

	public static final String DBCP_MAX_TOTAL = "dbcp.maxtotal";
	public static final String DBCP_MAX_IDLE = "dbcp.maxidle";
	public static final String DBCP_MAX_WAIT_MILLIS = "dbcp.maxwaitmillis";
	public static final String DBCP_TEST_ON_BORROW = "dbcp.testonborrow";
	public static final String DBCP_VALIDATION_QUERY = "dbcp.validationquery";

	public static final String FILE_FIELD_SEPERATOR = "file.field.seperator";
	public static final String FILE_FIELD_ORDER = "file.field.order";
	public static final String FILE_RECORD_SEPERATOR = "file.record.seperator";
	public static final String WORKING_DIRECTORY = "working.directory";
	public static final String SQLSERVER_SHARE = "sqlserver.share";
	public static final String INSERT_TABLE_NAME = "insert.table.name";
	public static final String INSERT_FORMAT_FILE = "insert.format.file";


	private Properties properties;

	public AdapterConfiguration() throws IOException {
		this("config.properties");
	}

	public AdapterConfiguration(String configFileName) throws IOException {
		logger.debug("Loading configuration from " + configFileName);
		File configFile = new File(configFileName);
	    FileReader reader = null;

		try {
		    reader = new FileReader(configFile);
		    properties = new Properties();
		    properties.load(reader);
		    
		    if(logger.isInfoEnabled()) {
		    	logger.info("Adapter configuration loaded :");
		    	Enumeration<?> e = properties.propertyNames();
				while (e.hasMoreElements()) {
					String key = (String) e.nextElement();
					String value = properties.getProperty(key);
					logger.info(key + " = " + value);
				}
		    }

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
