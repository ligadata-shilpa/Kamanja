package com.ligadata.dataGenerationTool;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.ligadata.dataGenerationTool.bean.ConfigObj;
import com.ligadata.dataGenerationTool.bean.FileNameConfig;

import org.apache.log4j.Logger;
import org.json.JSONObject;

public class MainClass {

	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws ParseException
	 */
	final static Logger logger = Logger.getLogger(MainClass.class);

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws InterruptedException,
			IOException, ParseException {
		// create object
		JsonUtility json = new JsonUtility();
		GenerateRecord record = new GenerateRecord();
		FilesUtility file = new FilesUtility();
		TimeUtility time = new TimeUtility();
		FileNameConfig fileNameConfig = new FileNameConfig();

		// read configuration file
		// logger.info("Reading config file from " + args[0]);
		String configFileLocation = args[0]; // "C:/Users/haitham-pc/Documents/GitHub/Kamanja/trunk/ContinuousTesting/JsonFiles/DataGenerationConfig.json";

		// DataGenerationConfig.json file
		JSONObject configJson = json.ReadJsonFile(configFileLocation);
		ConfigObj configObj = json.CreateConfigObj(configJson);

		// initialize variables
		String templateFileLocation = configObj.getTemplatePath();
		String destiniationDirectory = configObj.getDestiniationPath();

		// read message file
		JSONObject templateJson = json.ReadJsonFile(templateFileLocation);
		
		// DurationInHours
		double loopEndTime = time.RunDurationTime(configObj);
		HashMap<String, String> fieldsHash = new HashMap<String, String>();
		List<String> fieldsList;
		String hit;

		while (System.currentTimeMillis() < loopEndTime) {
			fieldsHash = json.ReadMessageFields(templateJson, configObj);

			if (configObj.getMessageType().equalsIgnoreCase("kv")) {
				hit = record.GenerateHitAsKV(fieldsHash,configObj.getDelimiter());
			} else {
				fieldsList = new ArrayList<String>(fieldsHash.values());
				hit = record.GenerateHitCSV(fieldsList,configObj.getDelimiter());
			}

			if (configObj.isDropInFiles()) {
				// write hit to file
				file.writeFile(hit, destiniationDirectory, configObj,fileNameConfig);
			} else if (configObj.isPushToKafka()) {
				// code to push to kafka
			}
			Thread.sleep(time.SleepTime(configObj.getDataGenerationRate()));
		}
		logger.info("Done ...");
	}

}
