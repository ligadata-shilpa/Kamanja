package com.ligadata.adapters.jdbc;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.ligadata.adapters.AdapterConfiguration;
import com.ligadata.adapters.BufferedMessageProcessor;

public class BufferedJDBCSink implements BufferedMessageProcessor {

  private Connection             connection;
  private PreparedStatement      statement;
  private final List<JSONObject> array;
  private String                 table;

  public BufferedJDBCSink() {
    array = new ArrayList<JSONObject>();
  }

  @Override
  public void init(AdapterConfiguration config) throws Exception {
    Class.forName(config.getProperty(AdapterConfiguration.JDBC_DRIVER));
    connection =
        DriverManager.getConnection(
            "jdbc:sqlserver://" + config.getProperty(AdapterConfiguration.JDBC_URL) + ":"
                + config.getProperty(AdapterConfiguration.JDBC_PORT) + ";databaseName="
                + config.getProperty(AdapterConfiguration.JDBC_DATABASE) + ";",
            config.getProperty(AdapterConfiguration.JDBC_USER),
            config.getProperty(AdapterConfiguration.JDBC_PASSWORD));

    table = config.getProperty(AdapterConfiguration.JDBC_TABLE);
    connection.setAutoCommit(false);
    // statement =
    // connection.prepareStatement(config.getProperty(AdapterConfiguration.JDBC_INSERT_STATEMENT));

  }

  @Override
  public void addMessage(String message) {
    // TODO parser json and bind values to insert statement
    try {
      // JsonNode jsonNode = new ObjectMapper().readTree(message);

      JSONParser jsonParser = new JSONParser();
      JSONObject jsonObject = (JSONObject) jsonParser.parse(message);

      array.add(jsonObject);

    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  @Override
  public void processAll() throws Exception {

    statement =
        connection
            .prepareStatement("INSERT INTO ["
                + table
                + "] ([ID],[APPID],[TIMEZONE],[PERSON_ID],[AUTHENTICATIONMETHOD],[BUSINESSFUNCTION],[CLIENTIP],[ACTIONCODE],[CORRID],[CORRIDPARENT],[DEVICE],[ERRORMESSAGE],[MESSAGETEXT],[CONFIDENTIALDATALABELS],[CONFIDENTIALRECORDCOUNT],[PROPRIETARYDATALABELS],[PROPRIETARYDATAVALUES],[PROPRIETARYRECORDCOUNT],[PROCESSID],[PROCESSNAME],[RESOURCE],[RESOURCEHOST],[RESOURCEPORT],[RESOURCEPROTOCOL],[RESOURCETYPE],[RESULT],[TYPECODE],[USERID],[SYSTEMUSER],[USERROLE],[EMP_FRST_NM],[EMP_LST_NM],[APP_NM],[SUM_NP],[SUM_PI],[DQ_SCORE],[EXTRA]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

    for (JSONObject jo : array) {
      JSONObject details = ((JSONObject) jo.get("details"));
      statement.setString(1, jo.get("Id").toString());
      statement.setInt(2, Integer.parseInt(jo.get("applicationId").toString()));
      statement.setString(3, jo.get("timezone").toString());
      statement.setInt(4, Integer.parseInt(details.get("personNumber").toString()));
      statement.setBinaryStream(5, new ByteArrayInputStream(details.get("authenticationMethod")
          .toString().getBytes(StandardCharsets.UTF_8)), details.get("authenticationMethod")
          .toString().getBytes().length);
      statement.setString(6, details.get("businessFunction").toString());
      statement.setString(7, details.get("clientIp").toString());
      statement.setString(8, details.get("action").toString());
      statement.setString(9, jo.get("correlationId").toString());
      statement.setString(10, jo.get("correlationParentId").toString());
      statement.setString(11, details.get("device").toString());
      statement.setString(12, details.get("errorMessage").toString());
      statement.setString(13, details.get("message").toString());
      statement.setString(14, details.get("confidentialDataLabels").toString());
      statement.setInt(15, Integer.parseInt(details.get("confidentialRecordCount").toString()));
      statement.setString(16, details.get("proprietaryDataLabels").toString());
      statement.setString(17, details.get("proprietaryDataValues").toString());
      statement.setInt(18, Integer.parseInt(details.get("proprietaryRecordCount").toString()));
      statement.setInt(19, Integer.parseInt(details.get("processId").toString()));
      statement.setString(20, details.get("processName").toString());
      statement.setString(21, details.get("resource").toString());
      statement.setString(22, details.get("resourceHost").toString());
      statement.setString(23, details.get("resourcePort").toString());
      statement.setString(24, details.get("resourceProtocol").toString());
      statement.setString(25, details.get("resourceType").toString());
      statement
          .setString(26, details.get("result") == null ? "" : details.get("result").toString());
      statement.setString(27, details.get("type").toString());
      statement.setString(28, details.get("user").toString());
      statement.setString(29, details.get("systemUser").toString());
      statement.setString(30, details.get("userRole").toString());
      statement.setString(31, details.get("firstName").toString());
      statement.setString(32, details.get("lastName").toString());
      statement.setString(33, details.get("typ_1_ct").toString());
      statement.setInt(34, Integer.parseInt(details.get("sumNP").toString()));
      statement.setInt(35, Integer.parseInt(details.get("sumPI").toString()));
      statement.setInt(36, Integer.parseInt(jo.get("dqscore").toString()));
      statement.setString(37, jo.get("extra") == null ? "" : jo.get("extra").toString());

      statement.addBatch();
    }

    statement.executeBatch();
    connection.commit();
  }

  @Override
  public void clearAll() {
    array.clear();
  }

  @Override
  public void close() {
    try {
      connection.close();
    } catch (SQLException e) {
    }
  }

}
