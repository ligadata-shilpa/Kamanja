package com.ligadata.dataGenerationTool;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.ligadata.dataGenerationTool.bean.ConfigObj;
import com.ligadata.dataGenerationTool.fields.CopsFields;

public class JsonUtility {

	final Logger logger = Logger.getLogger(JsonUtility.class);

	public JSONObject ReadJsonFile(String messageFileString) {
		logger.info("Reading JSON file...");
		BufferedReader bufferedReader;
		JSONObject req = null;
		try {
			bufferedReader = new BufferedReader(new FileReader(
					messageFileString));
			StringBuffer stringBuffer = new StringBuffer();
			String line = null;

			while ((line = bufferedReader.readLine()) != null) {

				stringBuffer.append(line).append("\n");
			}
			bufferedReader.close();
			String configJsonString = stringBuffer.toString();
			req = new JSONObject(configJsonString);
			return req;
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e);

		} finally {
			logger.info("Reading JSON file successful.");
		}
		return req;

	}

	public HashMap<String, String> ReadMessageFields(JSONObject req,
			ConfigObj configObj) {

		CopsFields copsFields = new CopsFields();
		String[] enumKeys = { "eventsource", "action", "resourceprotocol",
				"authenticationmethod", "resourcetype",
				"confidentialdatalabels", "userrole" };
		boolean lineAlreadyWritten;
		String value = "";

		RandomGenerator random = new RandomGenerator();
		JSONObject locs = req.getJSONObject("fields");
		JSONArray recs = locs.getJSONArray("field");
		HashMap<String, String> fields = new HashMap<String, String>();
		try {
			for (int i = 0; i < recs.length(); ++i) {
				lineAlreadyWritten = false;
				JSONObject rec = recs.getJSONObject(i);
				String fieldName = rec.getString("name");
				String fieldType = rec.getString("type");
				int fieldLength = rec.getInt("length");

				for (String str : enumKeys) {
					if (fieldName.equalsIgnoreCase(str)) {
						value = copsFields.EnumLookup(str).toString();
						lineAlreadyWritten = true;
						// break;
					}
				}

				if (lineAlreadyWritten == false) {
					value = random.CheckType(fieldType, fieldLength, configObj);
				}

				if (fieldName.equalsIgnoreCase("user")) {
					long temp = configObj.getSequenceID();
					configObj.setSequenceID(temp + 1);
					value = value + temp;

				}

				// if (fieldType.equalsIgnoreCase("timestamp")) {
				// value = value.substring(0, value.length() - 3) + "z";
				// }
				fields.put(fieldName, value);
			} // end for loop
		} catch (ParseException e) {
			e.printStackTrace();
			logger.error(e);
		}
		return fields;
	}

	public ConfigObj CreateConfigObj(JSONObject configJson) {
		logger.info("Parsing JSON object to config object...");
		ConfigObj configObj = new ConfigObj();
		configObj.setDataGenerationRate(configJson
				.getDouble("DataGenerationRate"));
		configObj.setStartDate(configJson.getString("StartDate"));
		configObj.setEndDate(configJson.getString("EndDate"));
		configObj.setDurationInHours(configJson.getDouble("DurationInHours"));
		configObj.setDropInFiles(configJson.getBoolean("DropInFiles"));
		configObj.setPushToKafka(configJson.getBoolean("PushToKafka"));
		configObj.setFileSplitPer(configJson.getString("FileSplitPer"));
		configObj.setDelimiter(configJson.getString("Delimiter"));
		configObj.setTemplatePath(configJson.getString("TemplatePath"));
		configObj.setDestiniationPath(configJson.getString("DestiniationPath"));
		configObj.setCompressFormat(configJson.getString("CompressFormat"));
		configObj.setSequenceID(configJson.getLong("SequenceId"));
		logger.info("Value of DataGenerationRate: "
				+ configJson.getDouble("DataGenerationRate"));
		logger.info("Value of StartDate: " + configJson.getString("StartDate"));
		logger.info("Value of EndDate: " + configJson.getString("EndDate"));
		logger.info("Value of DurationInHours: "
				+ configJson.getDouble("DurationInHours"));
		logger.info("Value of DropInFiles: "
				+ configJson.getBoolean("DropInFiles"));
		logger.info("Value of PushToKafka: "
				+ configJson.getBoolean("PushToKafka"));
		logger.info("Value of FileSplitPer: "
				+ configJson.getString("FileSplitPer"));
		logger.info("Value of TemplatePath: "
				+ configJson.getString("TemplatePath"));
		logger.info("Value of LogFilePath: "
				+ configJson.getString("DestiniationPath"));
		logger.info("Value of SequenceID: "
				+ configJson.getString("SequenceId"));
		logger.info("Parsing JSON object to config object successful.");
		return configObj;
	}
}
