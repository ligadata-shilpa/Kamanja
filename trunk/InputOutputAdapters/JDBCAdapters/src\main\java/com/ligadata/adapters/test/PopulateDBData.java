package com.ligadata.adapters.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.ligadata.adapters.utility.WrappedScheduleExecutor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PopulateDBData {
	
	private static JSONObject localConfigs;
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
	
	public static void main(String args[]){
		
		Options options = new Options();
		OptionGroup oGroup = new OptionGroup();
		Option opt = Option.builder("p")
				.required(true)
				.longOpt("props")
				.desc("Location of DB Properties File")
				.argName("Props File Location")
				.hasArg(true)
				.build();
		oGroup.addOption(opt);
		options.addOptionGroup(oGroup);
		
		CommandLineParser cmdParser = new DefaultParser();
		try {
			CommandLine cmdLine = cmdParser.parse( options, args);
			
			if(cmdLine != null && cmdLine.hasOption("p")) {
				String fileLoc = cmdLine.getOptionValue("p");
				
				File f = new File(fileLoc);
				if(!f.exists()){
					log.error("Unable to locate the DB Props file...");
					System.exit(-1);
					
				}else{
					BufferedReader reader = new BufferedReader(new FileReader(f));
					StringBuilder b = new StringBuilder();
					String line = reader.readLine();
					while(line != null){
						b.append(line);
						line = reader.readLine();
					}
					
					JSONParser parser = new JSONParser();
					JSONObject config = null;
					config =  (JSONObject)parser.parse(b.toString());
					if(config != null && config.size() >0){
						localConfigs = config;
						Long interval = (Long)config.get("interval");
						String timeUnits = (String)config.get("intervalUnits");
						TimeUnit unit = TimeUnit.valueOf(timeUnits.toUpperCase());
						
						WrappedScheduleExecutor scheduler = new WrappedScheduleExecutor(1);
						scheduler.scheduleAtFixedRate(new Runnable() {
							
							@Override
							public void run() {
								// TODO Auto-generated method stub
								String dbUser = (String)localConfigs.get("dbUser");
								String dbPwd = (String)localConfigs.get("dbPwd");
								String dbName = (String)localConfigs.get("dbName");
								String dbDriver = (String)localConfigs.get("dbDriver");
								String dbURL = (String)localConfigs.get("dbURL");
								String tableName = (String)localConfigs.get("tableName");
								
								String tableScript = "CREATE TABLE IF NOT EXISTS "+tableName
										+ " ( rownum bigint auto_increment primary key,"
										+ "emp_id bigint ,"
										+ "first_name varchar(256) ,"
										+ "last_name varchar(256) ,"
										+ "application_name varchar(256),"
										+ "access_time datetime,"
										+ "added_date datetime"
										+ ") engine=InnoDB default charset latin1";
								
								boolean tableExists = false;
								Connection con = null;
								PreparedStatement pstmt = null;
								ResultSet rs = null;
								
								System.out.println("Inserting data at "+sdf.format(new Date()));
								
								try {
									Class.forName(dbDriver);
									
									con = DriverManager.getConnection(dbURL+"/"+dbName, dbUser, dbPwd);
									
									if(con!=null){
										System.out.println("Connected to the DB");
										DatabaseMetaData dbmd = con.getMetaData();
										rs = dbmd.getTables(null, null, "%", null);
										
										//Look up for the tableName
										while(rs!=null && rs.next()){
											if(rs.getString(3).equalsIgnoreCase(tableName)){
												tableExists = true;
												break;
											}
										}
										
										//Create Table if it doesnt exist
										if(!tableExists){
											Statement stmt = con.createStatement();
											stmt.execute(tableScript);
											if(stmt.getUpdateCount() == 0 ){
												System.out.println("Created table "+tableName+" successfully...");
											}
										}else{
											System.out.println("Table "+tableName+" exists, skiping creation processs...");
										}
										
										pstmt = con.prepareStatement("INSERT INTO "+tableName+" (emp_id, first_name, last_name, application_name, access_time, added_date  ) "
												+ "VALUES (?, ?, ? , ? , CURRENT_TIMESTAMP , CURRENT_TIMESTAMP ) ");
										con.setAutoCommit(false);
										//Now pump in 50k Records
										Random randomGenerator1 = new Random();
										Random randomGenerator2 = new Random();
										Random randomGenerator3 = new Random();
										for(int j=0;j<10000; j++){
											int randomName = randomGenerator1.nextInt(100);
											int randomApp = randomGenerator2.nextInt(50);
											int randomID = randomGenerator3.nextInt(1000);
											pstmt.setLong(1, randomID);
											pstmt.setString(2, "fname"+randomName);
											pstmt.setString(3, "lname"+randomName);
											pstmt.setString(4, "app"+randomApp);
											int rows  = pstmt.executeUpdate();
											
											if(j % 1000 == 0)
												System.out.println("Inserting record "+j);
											
											if(rows == 1)
												continue;
											else
												break;
										}
										con.commit();
										
										rs.close();
										rs = null;
										pstmt.close();
										pstmt = null;
										con.close();
										con = null;
										
									}else{
										System.out.println("Unable to connect to the DB");
									}
								}catch (ClassNotFoundException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}catch (SQLException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}finally{
									try{
										if(rs != null) rs.close();
										if(pstmt != null) pstmt.close();
										if(con != null) con.close();
									}catch(SQLException exc){
										exc.printStackTrace();
										
									}
								}
							}
						}, 0, interval, unit);
					}
				}
			}else{
				log.error("Invalid Options....");
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp( "java -jar com.ligadata.adapters.test.PopulateDBData", options );
			}
		} catch (ParseException | IOException | org.json.simple.parser.ParseException e1) {
			// TODO Auto-generated catch block
			log.error("Invalid Options...."+e1.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "java -jar com.ligadata.adapters.test.PopulateDBData", options );
		}
	}
}
