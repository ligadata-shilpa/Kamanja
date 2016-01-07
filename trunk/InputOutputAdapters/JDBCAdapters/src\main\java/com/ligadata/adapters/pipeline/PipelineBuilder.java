package com.ligadata.adapters.pipeline;

import javax.sql.DataSource;

import org.easybatch.core.reader.RecordReader;
import org.json.simple.JSONObject;

import com.ligadata.adapters.processor.DelimitedRecordProcessor;
import com.ligadata.adapters.processor.JSONRecordProcessor;
import com.ligadata.adapters.processor.StringMapProcessor;
import com.ligadata.adapters.record.CustomRecordReader;
import com.ligadata.adapters.record.QueryReader;
import com.ligadata.adapters.record.TableReader;
import com.ligadata.adapters.utility.DBValidator;
import com.ligadata.adapters.writer.CustomRecordWriter;
import com.ligadata.adapters.writer.SimpleFileRecordWriter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PipelineBuilder {
	
	public static CustomRecordReader getRecordReader(JSONObject config){
		CustomRecordReader reader = null;
		String type=(String)config.get("type");
		if(type!=null && type.length()>0){
			String dbUser = (String)config.get("dbUser");
			String dbPwd = (String)config.get("dbPwd");
			String dbName = (String)config.get("dbName");
			String dbDriver = (String)config.get("dbDriver");
			String dbURL = (String)config.get("dbURL");
			//Append the DB Name to URL if it exists
			if(dbName!=null && dbName.length()>0)
				dbURL = dbURL+"/"+dbName;
			if(DBValidator.validateConnectivity(dbUser, dbPwd, dbDriver, dbURL)){
				log.debug("DB connection successful");
				DataSource ds = DBValidator.getDataSource(dbUser, dbPwd, dbDriver, dbURL);
				
				String tableName = (String)config.get("tableName");
				String query = (String)config.get("query");
				
				if(tableName!=null && tableName.length()>0){
					//Create a TableReader here
					String columns = (String)config.get("columns");
					String whereClause = (String)config.get("whereClause");
					if(DBValidator.validateTable(ds, tableName, columns, whereClause)){
						TableReader tReader = new TableReader(ds,tableName,columns);
						tReader.setWhereClause(whereClause);
						String temporalColumn = (String)config.get("temporalColumn");
						tReader.setTemporalColumn(temporalColumn);
						reader = tReader;
					}else{
						//Invalid Table and related parameters - Handle Errors here
					}
				}else if(query!=null && query.length()>0){
					//Create a QueryReader here
					if(DBValidator.validateQuery(ds, query)){
						QueryReader qReader = new QueryReader(ds, query);
						String temporalColumn = (String)config.get("temporalColumn");
						qReader.setTemporalColumn(temporalColumn);
						reader = qReader;
					}else{
						//Invalid Query - Handle Errors Here
					}
				}else{
					//Invalid Options - Handle Errors Here
				}
			}else{
				//Invalid DB Connectivity - Handle Error here
			}
		}
		return reader;
	}
	
	public static StringMapProcessor getRecordProcessor(JSONObject config){
		StringMapProcessor processor = null;
		String type = (String)config.get("type");
		if(type!=null && type.length()>0){
			String uType = type.toUpperCase();
			switch(uType){
				case "DELIMITED":
					DelimitedRecordProcessor dProcessor = new DelimitedRecordProcessor();
					String keyValueDelimiter = (String)config.get("keyValueDelimiter");
					String columnDelimiter = (String)config.get("keyValueDelimiter");
					String rowDelimiter = (String)config.get("rowDelimiter");
					dProcessor.setAddKeys(true);
					dProcessor.setKeyValueDelimiter(keyValueDelimiter);
					dProcessor.setColumnDelimiter(columnDelimiter);
					dProcessor.setRowDelimiter(rowDelimiter);
					processor = dProcessor;
					break;
				case "JSON":
					JSONRecordProcessor jProcessor = new JSONRecordProcessor();
					processor = jProcessor;
					break;
			}
		}else{
			//Invalid Processor type
		}
		return processor;
	}
	
	public static CustomRecordWriter getRecordWriter(JSONObject config){
		CustomRecordWriter writer = null;
		String type = (String)config.get("type");
		if(type!=null && type.length()>0){
			String uType = type.toUpperCase();
			switch(uType){
			case "FILE":
				SimpleFileRecordWriter sWriter = new SimpleFileRecordWriter();
				String fileName = (String)config.get("name");
				sWriter.setFileName(fileName);
				String compress = (String)config.get("compress");
				sWriter.setZipOutput(compress.equalsIgnoreCase("true")?true:false);
				String append = (String)config.get("append");
				sWriter.setAppend(append.equalsIgnoreCase("true")?true:false);
				writer = sWriter;
				break;
			}
		}
		return writer;
	}
}
