package com.ligadata.adapters.scratch;

public class CommandLineUtils {
	
	public static String getPipeline(int num){
		StringBuilder builder = new StringBuilder();
		switch(num){
		case 1:
			builder.append("{"+
						"input:{"+
							"type:\"JDBC\","+
							"dbUser:\"kuser\","+
							"dbPwd:\"centuser\","+
							"dbURL:\"jdbc:mariadb://192.168.1.8:3306\","+
							"dbDriver:\"org.mariadb.jdbc.Driver\","+
							"dbName:\"kamanja\","+
							"tableName:\" testTable\","+
							"columns:\" emp_id, first_name, last_name, application_name, access_time, added_date\","+
							"whereClause:\" where emp_id=12 \","+
							"temporalColumn:\"added_date\""+
						"},"+
						"processor:{"+
							"type:\"Delimited\","+
							"keyValueDelimiter:\":\","+
							"columnDelimiter:\",\","+
							"rowDelimiter:\";\""+
						"},"+
						"output:{"+
							"type:\"File\","+
							"name:\"test.txt\","+
							"compress:\"true\","+
							"append:\"false\""+
						"}"+
						"runInterval:\"10\","+
						"runUnits:\"seconds\""+
					"}");
			break;
		}
		return builder.toString();
	}
	
}
