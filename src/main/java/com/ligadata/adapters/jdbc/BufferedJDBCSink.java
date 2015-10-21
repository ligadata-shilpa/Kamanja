package com.ligadata.adapters.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.ligadata.adapters.AdapterConfiguration;

public class BufferedJDBCSink extends AbstractJDBCSink {
	static Logger logger = Logger.getLogger(BufferedJDBCSink.class);

  private PreparedStatement      insertStatement;
  private List<ParameterMapping> insertParams;

  public BufferedJDBCSink() {}

  @Override
  public void init(AdapterConfiguration config) throws Exception {
    super.init(config);

    insertParams = new ArrayList<ParameterMapping>();
    String insertStr = config.getProperty(AdapterConfiguration.JDBC_INSERT_STATEMENT);
    if (insertStr == null)
      throw new Exception("Insert statement not specified in the properties file.");

	logger.info("Insert statement: " + insertStr);
    insertStatement = buildStatementAndParameters(insertStr, insertParams);
	if(logger.isInfoEnabled()) {
		logger.info(insertParams.size() + " parameters found.");
		int i = 1;
		for (ParameterMapping param : insertParams)
			logger.info("Parameter " + (i++) + ": path=" + Arrays.toString(param.path) + " type=" + param.type + " typeName=" + param.typeName);
	}
  }

  @Override
  public boolean addMessage(String message) {

    try {
      JSONParser jsonParser = new JSONParser();
      JSONObject jsonObject = (JSONObject) jsonParser.parse(message);

      if(jsonObject.get("dedup") != null && "1".equals(jsonObject.get("dedup").toString())) {
			logger.debug("ignoring duplicate message.");
			return false;
		}
      
      if(bindParameters(insertStatement, insertParams, jsonObject))
    	  insertStatement.addBatch();
    } catch (Exception e) {
      logger.error("Error processing message - ignoring message : " + e.getMessage(), e);
      return false;
    }
    
    return true;
  }

  @Override
  public void processAll() throws Exception {
    try {
      logger.info("Saving messages to database");
      insertStatement.executeBatch();
      connection.commit();
    } catch (Exception e) {
      logger.error("Error processing message - ignoring message : " + e.getMessage(), e);
      connection.commit();
    }
  }

  @Override
  public void clearAll() {
    try {
      if (insertStatement != null)
        insertStatement.clearBatch();
    } catch (SQLException e) {
        logger.error("Error processing message - ignoring message : " + e.getMessage(), e);
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
