package com.ligadata.dataGenerationTool;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import com.ligadata.dataGenerationToolBean.ConfigObj;

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
		RandomGenerator random = new RandomGenerator();
		JSONObject locs = req.getJSONObject("fields");
		JSONArray recs = locs.getJSONArray("field");
		HashMap<String, String> fields = new HashMap<String, String>();
		try {
			for (int i = 0; i < recs.length(); ++i) {
				JSONObject rec = recs.getJSONObject(i);
				String fieldName = rec.getString("name");
				String fieldType = rec.getString("type");
				int fieldLength = rec.getInt("length");
				String value = random.CheckType(fieldType, fieldLength,
						configObj);
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
		logger.info("Parsing JSON object to config object successful.");
		return configObj;
	}
}
