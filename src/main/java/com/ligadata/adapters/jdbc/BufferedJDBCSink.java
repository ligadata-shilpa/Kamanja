package com.ligadata.adapters.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.ligadata.adapters.AdapterConfiguration;
import com.ligadata.adapters.BufferedMessageProcessor;

public class BufferedJDBCSink implements BufferedMessageProcessor {

	private Connection connection;
	private PreparedStatement statement;
	private List<ParameterMapping> paramArray;
	private SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	private class ParameterMapping {
		String type;
		String[] path;

		private ParameterMapping(String type, String[] path) {
			this.type = type;
			this.path = path;
		}
	}

	public BufferedJDBCSink() {
		// array = new ArrayList<JSONObject>();
	}

	@Override
	public void init(AdapterConfiguration config) throws Exception {
		Class.forName(config.getProperty(AdapterConfiguration.JDBC_DRIVER));
		connection = DriverManager.getConnection(config.getProperty(AdapterConfiguration.JDBC_URL),
				config.getProperty(AdapterConfiguration.JDBC_USER),
				config.getProperty(AdapterConfiguration.JDBC_PASSWORD));

		connection.setAutoCommit(false);
		int countparameters = StringUtils.countMatches(config.getProperty(AdapterConfiguration.JDBC_INSERT_STATEMENT), "?");
		paramArray = new ArrayList<ParameterMapping>();
		inputFormat = new SimpleDateFormat(config.getProperty(AdapterConfiguration.INPUT_DATE_FORMAT, "yyyy-MM-dd"));

		for (int i = 1; i <= countparameters; i++) {
			String param = config.getProperty(AdapterConfiguration.JDBC_PARAM + i);
			if (param == null || "".equals(param))
				throw new Exception("Missing configuration for insert statement parameter " + i);

			String[] typeandparam = param.split(",");
			String[] jsontree = typeandparam[1].split("\\.");
			paramArray.add(new ParameterMapping(typeandparam[0], jsontree));
		}

		statement = connection.prepareStatement(config.getProperty(AdapterConfiguration.JDBC_INSERT_STATEMENT));
	}

	@Override
	public void addMessage(String message) {
		int paramIndex = 0;
		String key = null;
		String value = null;
		try {

			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = (JSONObject) jsonParser.parse(message);

			// array.add(jsonObject);

			paramIndex = 1;
			for (ParameterMapping param : paramArray) {
				key = param.path.toString();

				// traverse JSON tree to get the value
				JSONObject subobject = jsonObject;
				for (int i = 0; i < param.path.length - 1; i++) {
					if (subobject != null)
						subobject = ((JSONObject) subobject.get(param.path[i]));
				}
				value = null;
				if(subobject != null && subobject.get(param.path[param.path.length-1]) != null)
					value = subobject.get(param.path[param.path.length-1]).toString();

				key = param.path.toString();
				
				if (param.type.equals("1")) { 
					//String
					statement.setString(paramIndex, value);
				} else if (param.type.equals("2")) {
					//Integer
					if(value == null || "".equals(value))
						statement.setNull(paramIndex, java.sql.Types.INTEGER);
					else
						statement.setInt(paramIndex, Integer.parseInt(value));
				} else if (param.type.equals("3")) {
					// Double
					if(value == null || "".equals(value))
						statement.setNull(paramIndex, java.sql.Types.DOUBLE);
					else
						statement.setDouble(paramIndex, Double.parseDouble(value));
				} else if (param.type.equals("4")) {
					// Date
					java.sql.Date dt = null;
					if(value == null || "".equals(value))
						statement.setNull(paramIndex, java.sql.Types.DATE);
					else {
						java.util.Date date = inputFormat.parse(value);
						dt = new java.sql.Date(date.getTime());
						statement.setDate(paramIndex, dt);
					}
				} else if (param.type.equals("5")) {
					// timestamp
					Timestamp ts = null;
					if(value == null || "".equals(value))
						statement.setNull(paramIndex, java.sql.Types.TIMESTAMP);
					else {
						java.util.Date date = inputFormat.parse(value);
						ts = new Timestamp(date.getTime());
						statement.setTimestamp(paramIndex, ts);
					}
				}
				paramIndex++;
			}
			statement.addBatch();
		} catch (Exception e) {
			System.out.println("Error processing message - ignoring message : " + e.getMessage());
			System.out.println("Error for Parameter index : [" + paramIndex + "] Key : [" + key + "] value : [" + value + "]");
			e.printStackTrace();
		}

	}

	@Override
	public void processAll() throws Exception {
		System.out.println("Thread " + Thread.currentThread().getId() + ": Saving messages to database");
		statement.executeBatch();
		connection.commit();
	}

	@Override
	public void clearAll() {
		try {
			if (statement != null)
				statement.clearBatch();
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

}
