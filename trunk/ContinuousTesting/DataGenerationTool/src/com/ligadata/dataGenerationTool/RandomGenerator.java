package com.ligadata.dataGenerationTool;

import com.ligadata.dataGenerationTool.bean.ConfigObj;
import org.apache.commons.lang3.RandomStringUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class RandomGenerator {

    public String GenerateRandomRecord(String FieldType, int length, double minVal, double maxVal, ConfigObj configObj)
            throws ParseException {
        String randomValue = null;
        switch (FieldType.toLowerCase().trim()) {
            case "hybrid": // this is not used anymore.
                randomValue = RandomHybrid(length);
                break;
            case "empty":
                randomValue = "";
                break;
            case "string":
                randomValue = RandomString(length);
                break;
            case "char":
                randomValue = RandomString(1);
                break;
            case "double":
                double randomDouble = ThreadLocalRandom.current().nextDouble(minVal, maxVal);
                randomValue = (Math.round(randomDouble * 100.0) / 100.0) + "";
                break;
            case "int":
                Random rn = new Random();
                randomValue = String.valueOf(rn.nextInt((int) (maxVal - minVal + 1)) + (int) minVal);
                break;
            case "long":
                Random rn1 = new Random();
                randomValue = String.valueOf(minVal + (long) (rn1.nextDouble() * (maxVal - minVal)));
                break;
            case "timestamp":
                String temp = RandomDateBetweenTwoDate(configObj.getStartDate(),
                        configObj.getEndDate(), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSz");
                randomValue = temp.substring(0, temp.length() - 3) + "z";
                break;
            case "date":
                randomValue = RandomDateBetweenTwoDate(configObj.getStartDate(),
                        configObj.getEndDate(), "yyyy-MM-dd");
                break;
            case "date2":
                randomValue = RandomDateBetweenTwoDate(String.valueOf((long) minVal),
                        String.valueOf((long) maxVal), "yyyyMMdd");
                randomValue = randomValue.replaceAll("-", "");
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
