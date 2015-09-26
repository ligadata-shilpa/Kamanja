package com.ligadata.adapters.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.ligadata.adapters.AdapterConfiguration;
import com.ligadata.adapters.BufferedMessageProcessor;

public class BufferedJDBCSink implements BufferedMessageProcessor{

	private Connection connection;
	private PreparedStatement statement;

	@Override
	public void init(AdapterConfiguration config) throws Exception {
		 Class.forName(config.getProperty(AdapterConfiguration.JDBC_DRIVER));
         connection = DriverManager.getConnection(config.getProperty(AdapterConfiguration.JDBC_URL),
        		 config.getProperty(AdapterConfiguration.JDBC_USER), config.getProperty(AdapterConfiguration.JDBC_PASSWORD));
         
         connection.setAutoCommit(false);
         statement = connection.prepareStatement(config.getProperty(AdapterConfiguration.JDBC_INSERT_STATEMENT));
		
	}

	@Override
	public void addMessage(String message) {
		// TODO parser json and bind values to insert statement
		try {
			JsonNode jsonNode = new ObjectMapper().readTree(message);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void processAll() throws Exception {
		int[] count = statement.executeBatch();
		connection.commit();
	}

	@Override
	public void clearAll() {		
	}
	
	@Override
	public void close() {
		try {
			connection.close();
		} catch (SQLException e) {
		}
	}

}
