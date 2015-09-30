package com.ligadata.adapters.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.ligadata.adapters.AdapterConfiguration;
import com.ligadata.adapters.BufferedMessageProcessor;

public class BufferedJDBCSink implements BufferedMessageProcessor {

  private Connection             connection;
  private PreparedStatement      statement;
  private final List<JSONObject> array;
  private int                    countparameters;
  private List<String>           paramArray;

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

    connection.setAutoCommit(false);
    countparameters =
        StringUtils.countMatches(config.getProperty(AdapterConfiguration.JDBC_INSERT_STATEMENT),
            "?");
    paramArray = new ArrayList<String>();

    for (int i = 1; i <= countparameters; i++) {
      paramArray.add(config.getProperty(AdapterConfiguration.JDBC_PARAM + i));
    }

    statement =
        connection.prepareStatement(config.getProperty(AdapterConfiguration.JDBC_INSERT_STATEMENT));

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
    int counter = 0;
    String value = null;
    JSONObject subobject = new JSONObject();
    String type = null;

    for (JSONObject jo : array) {
      for (String param : paramArray) {
        counter++;

        String[] typeandparam = param.split(",");
        type = typeandparam[0];
        String[] jsontree = typeandparam[1].split("\\.");
        if (jsontree.length == 0) {
          value = jo.get(typeandparam[1]).toString();
        } else {
          int count = 0;
          for (String jskey : jsontree) {
            count++;
            if (jsontree.length == count) {
              if (subobject.get(jskey) != null) {
                value = subobject.get(jskey).toString();
              }
            } else {
              subobject = ((JSONObject) jo.get(jskey));
            }
          }
        }

        if (type.equals("1")) {
          statement.setString(counter, value);
        } else if (type.equals("2")) {
          statement.setInt(counter, value == null ? 0 :Integer.parseInt(value));
        }
      }
      counter = 0;
      statement.addBatch();
    }
    //
    // for (JSONObject jo : array) {
    // JSONObject details = ((JSONObject) jo.get("details"));
    // statement.setString(1, jo.get("Id").toString());
    // statement.setInt(2, Integer.parseInt(jo.get("applicationId").toString()));
    // statement.setString(3, jo.get("timezone").toString());
    // statement.setInt(4, Integer.parseInt(details.get("personNumber").toString()));
    // statement.setBinaryStream(5, new ByteArrayInputStream(details.get("authenticationMethod")
    // .toString().getBytes(StandardCharsets.UTF_8)), details.get("authenticationMethod")
    // .toString().getBytes().length);
    // statement.setString(6, details.get("businessFunction").toString());
    // statement.setString(7, details.get("clientIp").toString());
    // statement.setString(8, details.get("action").toString());
    // statement.setString(9, jo.get("correlationId").toString());
    // statement.setString(10, jo.get("correlationParentId").toString());
    // statement.setString(11, details.get("device").toString());
    // statement.setString(12, details.get("errorMessage").toString());
    // statement.setString(13, details.get("message").toString());
    // statement.setString(14, details.get("confidentialDataLabels").toString());
    // statement.setInt(15, Integer.parseInt(details.get("confidentialRecordCount").toString()));
    // statement.setString(16, details.get("proprietaryDataLabels").toString());
    // statement.setString(17, details.get("proprietaryDataValues").toString());
    // statement.setInt(18, Integer.parseInt(details.get("proprietaryRecordCount").toString()));
    // statement.setInt(19, Integer.parseInt(details.get("processId").toString()));
    // statement.setString(20, details.get("processName").toString());
    // statement.setString(21, details.get("resource").toString());
    // statement.setString(22, details.get("resourceHost").toString());
    // statement.setString(23, details.get("resourcePort").toString());
    // statement.setString(24, details.get("resourceProtocol").toString());
    // statement.setString(25, details.get("resourceType").toString());
    // statement
    // .setString(26, details.get("result") == null ? "" : details.get("result").toString());
    // statement.setString(27, details.get("type").toString());
    // statement.setString(28, details.get("user").toString());
    // statement.setString(29, details.get("systemUser").toString());
    // statement.setString(30, details.get("userRole").toString());
    // statement.setString(31, details.get("firstName").toString());
    // statement.setString(32, details.get("lastName").toString());
    // statement.setString(33, details.get("typ_1_ct").toString());
    // statement.setInt(34, Integer.parseInt(details.get("sumNP").toString()));
    // statement.setInt(35, Integer.parseInt(details.get("sumPI").toString()));
    // statement.setInt(36, Integer.parseInt(jo.get("dqscore").toString()));
    // statement.setString(37, jo.get("extra") == null ? "" : jo.get("extra").toString());
    //
    // statement.addBatch();
    // }

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
