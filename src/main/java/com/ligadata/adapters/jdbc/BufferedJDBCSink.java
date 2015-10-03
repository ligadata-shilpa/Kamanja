package com.ligadata.adapters.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.ligadata.adapters.AdapterConfiguration;

public class BufferedJDBCSink extends AbstractJDBCSink {

	private PreparedStatement insertStatement;
	private List<ParameterMapping> insertParams;

	public BufferedJDBCSink() {
	}

	@Override
	public void init(AdapterConfiguration config) throws Exception {
		super.init(config);

		insertParams = new ArrayList<ParameterMapping>();
		String insertStr = config.getProperty(AdapterConfiguration.JDBC_INSERT_STATEMENT);
		if(insertStr == null)
			throw new Exception("Insert statement not specified in the properties file.");
		
		insertStatement = buildStatementAndParameters(insertStr, insertParams);		
	}

	@Override
	public void addMessage(String message) {
				
		try {
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = (JSONObject) jsonParser.parse(message);

			bindParameters(insertStatement, insertParams, jsonObject);
			insertStatement.addBatch();
		} catch (Exception e) {
			System.out.println("Error processing message - ignoring message : " + e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	public void processAll() throws Exception {
		System.out.println("Thread " + Thread.currentThread().getId() + ": Saving messages to database");
		insertStatement.executeBatch();
		connection.commit();
	}

	@Override
	public void clearAll() {
		try {
			if (insertStatement != null)
				insertStatement.clearBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		try {
			if (connection != null)
				connection.close();
		} catch (SQLException e) {
		}
	}

	public static void main(String[] args) {
		String rec1 = "{\"Id\": \"b0118f6e-59d1-4f5d-a198-f81c635a98f6\", \"applicationId\": \"\", \"createdAt\": \"2015-10-02 20:55:10\", \"timestamp\": \"2015-09-25T10:39:48.0000866Z\", \"timezone\": \"-04:00\", \"correlationId\": \"\", \"correlationParentId\": \"\", \"dqscore\": 100.0, \"details\": {\"sumNP\":\"3\",\"confidentialdatalabels\":\"firstname,lastname,ssntin\",\"type\":\"userActivityEvent\",\"personNumber\":\"NL\",\"ait\":\"972919\",\"sumPI\":\"1\",\"proprietarydatavalues\":\"standardid=abcdefg\",\"result\":\"0\",\"resourcehost\":\"lsdie1p8e.sdi.corp.bankofamerica.com\",\"resourceprotocol\":\"http\",\"clientip\":\"171.139.51.146\",\"action\":\"read\",\"systemuser\":\"nbeqwos\",\"timePartitionData\":\"0\",\"ApplicationName\":\"NL\",\"resource\":\"/CustomerWebApp/GetCustomerServlet.do\",\"processname\":\"http-bio-8080-exec-4\",\"resourceport\":\"8080\",\"resourcetype\":\"webapp\",\"proprietarydatalabels\":\"standardid\",\"transactionId\":\"0\",\"confidentialrecordcount\":\"1\",\"application\":\"Client360\",\"processid\":\"23\",\"user\":\"nbktest\",\"device\":\"lsdie1p8e.sdi.corp.bankofamerica.com\"}}";
		String rec2 = "{\"Id\": \"b0218f6e-59d1-4f5d-a198-f81c635a98f6\", \"applicationId\": \"\", \"createdAt\": \"2015-10-02 20:55:10\", \"timestamp\": \"2015-09-25T10:39:48.0000866Z\", \"timezone\": \"-04:00\", \"correlationId\": \"\", \"correlationParentId\": \"\", \"dqscore\": 100.0, \"details\": {\"sumNP\":\"3\",\"confidentialdatalabels\":\"firstname,lastname,ssntin\",\"type\":\"userActivityEvent\",\"personNumber\":\"NL\",\"ait\":\"972919\",\"sumPI\":\"1\",\"proprietarydatavalues\":\"standardid=abcdefg\",\"result\":\"0\",\"resourcehost\":\"lsdie1p8e.sdi.corp.bankofamerica.com\",\"resourceprotocol\":\"http\",\"clientip\":\"171.139.51.146\",\"action\":\"read\",\"systemuser\":\"nbeqwos\",\"timePartitionData\":\"0\",\"ApplicationName\":\"NL\",\"resource\":\"/CustomerWebApp/GetCustomerServlet.do\",\"processname\":\"http-bio-8080-exec-4\",\"resourceport\":\"8080\",\"resourcetype\":\"webapp\",\"proprietarydatalabels\":\"standardid\",\"transactionId\":\"0\",\"confidentialrecordcount\":\"1\",\"application\":\"Client360\",\"processid\":\"23\",\"user\":\"nbktest\",\"device\":\"lsdie1p8e.sdi.corp.bankofamerica.com\"}}";
		String rec3 = "{\"Id\": \"b0318f6e-59d1-4f5d-a198-f81c635a98f6\", \"applicationId\": \"\", \"createdAt\": \"2015-10-02 20:55:10\", \"timestamp\": \"2015-09-25T10:39:48.0000866Z\", \"timezone\": \"-04:00\", \"correlationId\": \"\", \"correlationParentId\": \"\", \"dqscore\": 100.0, \"details\": {\"sumNP\":\"3\",\"confidentialdatalabels\":\"firstname,lastname,ssntin\",\"type\":\"userActivityEvent\",\"personNumber\":\"NL\",\"ait\":\"972919\",\"sumPI\":\"1\",\"proprietarydatavalues\":\"standardid=abcdefg\",\"result\":\"0\",\"resourcehost\":\"lsdie1p8e.sdi.corp.bankofamerica.com\",\"resourceprotocol\":\"http\",\"clientip\":\"171.139.51.146\",\"action\":\"read\",\"systemuser\":\"nbeqwos\",\"timePartitionData\":\"0\",\"ApplicationName\":\"NL\",\"resource\":\"/CustomerWebApp/GetCustomerServlet.do\",\"processname\":\"http-bio-8080-exec-4\",\"resourceport\":\"8080\",\"resourcetype\":\"webapp\",\"proprietarydatalabels\":\"standardid\",\"transactionId\":\"0\",\"confidentialrecordcount\":\"1\",\"application\":\"Client360\",\"processid\":\"23\",\"user\":\"nbktest\",\"device\":\"lsdie1p8e.sdi.corp.bankofamerica.com\"}}";
		String rec4 = "{\"Id\": \"b0418f6e-59d1-4f5d-a198-f81c635a98f6\", \"applicationId\": \"\", \"createdAt\": \"2015-10-02 20:55:10\", \"timestamp\": \"2015-09-25T10:39:48.0000866Z\", \"timezone\": \"-04:00\", \"correlationId\": \"\", \"correlationParentId\": \"\", \"dqscore\": 100.0, \"details\": {\"sumNP\":\"3\",\"confidentialdatalabels\":\"firstname,lastname,ssntin\",\"type\":\"userActivityEvent\",\"personNumber\":\"NL\",\"ait\":\"972919\",\"sumPI\":\"1\",\"proprietarydatavalues\":\"standardid=abcdefg\",\"result\":\"0\",\"resourcehost\":\"lsdie1p8e.sdi.corp.bankofamerica.com\",\"resourceprotocol\":\"http\",\"clientip\":\"171.139.51.146\",\"action\":\"read\",\"systemuser\":\"nbeqwos\",\"timePartitionData\":\"0\",\"ApplicationName\":\"NL\",\"resource\":\"/CustomerWebApp/GetCustomerServlet.do\",\"processname\":\"http-bio-8080-exec-4\",\"resourceport\":\"8080\",\"resourcetype\":\"webapp\",\"proprietarydatalabels\":\"standardid\",\"transactionId\":\"0\",\"confidentialrecordcount\":\"1\",\"application\":\"Client360\",\"processid\":\"23\",\"user\":\"nbktest\",\"device\":\"lsdie1p8e.sdi.corp.bankofamerica.com\"}}";
		AdapterConfiguration config;
		try {
			config = new AdapterConfiguration("copsjdbc.properties");
			BufferedJDBCSink processor = new BufferedJDBCSink();
			processor.init(config);
			processor.addMessage(rec1);
			processor.addMessage(rec2);
			processor.addMessage(rec3);
			processor.addMessage(rec4);
			processor.processAll();
			processor.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
