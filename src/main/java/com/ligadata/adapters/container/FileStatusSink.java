package com.ligadata.adapters.container;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.ligadata.adapters.AdapterConfiguration;
import com.ligadata.adapters.jdbc.AbstractJDBCSink;

public class FileStatusSink extends AbstractJDBCSink {
	static Logger logger = Logger.getLogger(FileStatusSink.class);

	private PreparedStatement statement;
	
	@Override
	public void init(AdapterConfiguration config) throws Exception {
		super.init(config);

		String sqlStr = config.getProperty(AdapterConfiguration.JDBC_INSERT_STATEMENT);
		if(sqlStr == null)
			throw new Exception("Sql statement not specified in the properties file.");
		
		logger.info("Sql statement: " + sqlStr);
		statement = connection.prepareCall(sqlStr);
	}

	@Override
	public boolean addMessage(String message) {
		String[] fields = message.split(",");
		if(fields.length < 4) {
			logger.error("Incorrect message. Expecting atleast 4 fields. Message: " + message);
			return false;
		}
		
		try {
			if("File_Total_Result".equalsIgnoreCase(fields[0])) {			
				statement.setString(1, fields[2]);
				java.util.Date date = inputFormat.parse(fields[1]);
				java.sql.Date dt = new java.sql.Date(date.getTime());
				statement.setDate(2, dt);
				statement.setLong(3, Long.parseLong(fields[3]));
				statement.addBatch();
			}
		} catch (Exception e) {
			logger.error("Error: " + e.getMessage(), e);
			try { statement.clearParameters(); } catch (SQLException e1) {}
			return false;
		}
		
		return true;
	}

	@Override
	public void processAll() throws Exception {		
	    try {
	        logger.info("Saving messages to database");
	        statement.executeBatch();
	        connection.commit();
	      } catch (Exception e) {
	        logger.error("Error processing message - ignoring message : " + e.getMessage(), e);
	        connection.commit();
	      }		
	}

	@Override
	public void clearAll() {
	    try {
	        if (statement != null)
	        	statement.clearBatch();
	      } catch (SQLException e) {
	          logger.error("Error processing message - ignoring message : " + e.getMessage(), e);
	      }
	}
}
