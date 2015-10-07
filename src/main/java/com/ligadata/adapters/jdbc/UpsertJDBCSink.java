package com.ligadata.adapters.jdbc;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.ligadata.adapters.AdapterConfiguration;

public class UpsertJDBCSink extends AbstractJDBCSink {

	private PreparedStatement insertStatement;
	private List<ParameterMapping> insertParams;
	private PreparedStatement updateStatement;
	private List<ParameterMapping> updateParams;

	public UpsertJDBCSink() {
	}
	
	@Override
	public void init(AdapterConfiguration config) throws Exception {
		super.init(config);

		insertParams = new ArrayList<ParameterMapping>();
		updateParams = new ArrayList<ParameterMapping>();
		String insertStr = config.getProperty(AdapterConfiguration.JDBC_INSERT_STATEMENT);
		if(insertStr == null)
			throw new Exception("Insert statement not specified in the properties file.");
		
		String updateStr = config.getProperty(AdapterConfiguration.JDBC_UPDATE_STATEMENT);
		if(updateStr == null)
			throw new Exception("Update statement not specified in the properties file.");
		
		insertStatement = buildStatementAndParameters(insertStr, insertParams);
		updateStatement = buildStatementAndParameters(updateStr, updateParams);   
	}

	@Override
	public void addMessage(String message) {
		try {
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = (JSONObject) jsonParser.parse(message);

			if(bindParameters(updateStatement, updateParams, jsonObject)) {
				updateStatement.execute();
				if(updateStatement.getUpdateCount() == 0) {
					if(bindParameters(insertStatement, insertParams, jsonObject))
						insertStatement.execute();
				}
					
				System.out.println("Thread " + Thread.currentThread().getId() + ": Saving message to database");
				connection.commit();
			}
			
		} catch (Exception e) {
			System.out.println("Error processing message - ignoring message : " + e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	public void processAll() throws Exception {
	}

	public static void main(String[] args) {
		String rec1 = "{\"appId\": \"unknownappid\", \"DQScore\": \"20.0\",\"datetime\": \"2015-09-25T10:39:45.0000132Z\", \"DailyAggs\": \"20.0\"}";
		String rec2 = "{\"appId\": \"123\", \"DQScore\": \"20.0\",\"datetime\": \"2015-09-25T10:39:45.0000132Z\", \"DailyAggs\": \"20.0\"}";
		String rec3 = "{\"appId\": \"unknownappid\", \"DQScore\": \"20.0\",\"datetime\": \"2015-09-25T10:39:45.0000132Z\", \"DailyAggs\": \"40.0\"}";
		String rec4 = "{\"appId\": \"123\", \"DQScore\": \"20.0\",\"datetime\": \"2015-09-25T10:39:45.0000132Z\", \"DailyAggs\": \"30.0\"}";
		AdapterConfiguration config;
		try {
			config = new AdapterConfiguration("dqjdbc.properties");
			UpsertJDBCSink processor = new UpsertJDBCSink();
			processor.init(config);
			processor.addMessage(rec1);
			processor.addMessage(rec2);
			processor.addMessage(rec3);
			processor.addMessage(rec4);
			processor.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
