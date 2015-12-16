package com.ligadata.dataGenerationTool;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.lang3.RandomStringUtils;

import com.ligadata.dataGenerationTool.bean.ConfigObj;

public class RandomGenerator {

	public String CheckType(String FieldType, int length, ConfigObj configObj)
			throws ParseException {
		String randomValue = null;
		switch (FieldType.toLowerCase().trim()) {
		case "hybrid":
			randomValue = RandomHybrid(length);
			break;
		case "string":
			randomValue = RandomString(length);
			break;
		case "char":
			randomValue = RandomString(1);
			break;
		case "double":
			int precision = 2;
			String randomNumric = RandomNumeric(length - precision);
			String randomNumric2 = RandomNumeric(precision);
			randomValue = randomNumric + "." + randomNumric2;
			break;
		case "integer":
			randomValue = RandomNumeric(length);
			break;
		case "long":
			randomValue = RandomNumeric(length);
			break;
		case "timestamp":
			randomValue = RandomDateBetweenTwoDate(configObj.getStartDate(),
					configObj.getEndDate(), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSz");
			break;
		case "date":
			randomValue = RandomDateBetweenTwoDate(configObj.getStartDate(),
					configObj.getEndDate(), "yyyy-MM-dd");
			break;
		default:
			randomValue = null;
			break;
		}
		return randomValue;
	}

	public String RandomHybrid(int length) {

		boolean useLetters = true;
		boolean useNumbers = true;
		String generatedAlphabetic = RandomStringUtils.random(length,
				useLetters, useNumbers);
		return new String(generatedAlphabetic);
	}

	public String RandomString(int length) {

		boolean useLetters = true;
		boolean useNumbers = false;
		String generatedString = RandomStringUtils.random(length, useLetters,
				useNumbers);
		return new String(generatedString);
	}

	public String RandomNumeric(int length) {

		String generatedNumeric = RandomStringUtils.randomNumeric(length);
		return new String(generatedNumeric);
	}

	public String RandomDateBetweenTwoDate(String strDate, String edDate,
			String fieldType) throws ParseException {
		DateFormat format = new SimpleDateFormat(fieldType, Locale.ENGLISH);
		Date startDate = format.parse(strDate);
		Date endDate = format.parse(edDate);

		long startTimeStamp = startDate.getTime();
		long endTimeStamp = endDate.getTime();

		long range = endTimeStamp - startTimeStamp;
		double result = (Math.random() * range) + startTimeStamp; // startDateCalendar.getTimeInMillis(),
																	// endDateCalendar.getTimeInMillis());

		Date d = new Date((long) result);

		DateFormat df = new SimpleDateFormat(fieldType);
		String resultDateString = df.format(d);

		return resultDateString;

	}

}
