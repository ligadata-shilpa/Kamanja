package com.ligadata.adapters.scratch;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.google.common.eventbus.AsyncEventBus;
import com.ligadata.adapters.processor.JSONRecordProcessor;
import com.ligadata.adapters.record.QueryReader;
import com.ligadata.adapters.utility.DBValidator;
import com.ligadata.adapters.utility.SimpleEventListener;
import com.ligadata.adapters.writer.JSONRecordWriter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JSONPipeline {
	public static void main(String[] args) {
		String dbUser = "kuser";
		String dbPwd = "centuser";
		String dbName = "kamanja";
		String dbDriver = "org.mariadb.jdbc.Driver";
		String dbURL = "jdbc:mariadb://192.168.1.8:3306";
		//String dbURL = "jdbc:mariadb://10.0.1.105:3306";
		
		String query = "Select emp_id, first_name, last_name, application_name, access_time, added_date "
				+ " from testTable "
				+ "  where added_date < current_date() ";
		
		QueryReader reader = null;
		
		long startTime = System.currentTimeMillis();
		
		if(DBValidator.validateConnectivity(dbUser, dbPwd, dbDriver, dbURL+"/"+dbName)){
			System.out.println("DB connection successful");
			DataSource ds = DBValidator.getDataSource(dbUser, dbPwd, dbDriver, dbURL+"/"+dbName);
			if(DBValidator.validateQuery(ds, query)){
				System.out.println("Query Test Successful");
				reader = new QueryReader(ds, query);
				reader.setFetchSize(10);
				reader.setQueryTimeout(60);
			}
		}
		
		ExecutorService service = Executors.newFixedThreadPool(4);
		AsyncEventBus eb = new AsyncEventBus("JDBC Ingester", service);
		
		JSONRecordWriter writer = new JSONRecordWriter("test.json",null,true);
		
		try {
			writer.open();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		if(reader!= null){
			
			SimpleEventListener listener = new SimpleEventListener(writer);
			eb.register(listener);
			
			JSONRecordProcessor jsonProcessor = new JSONRecordProcessor();
			Thread thread = new Thread(new JDBCReaderThread(reader, eb, "Query Thread",jsonProcessor));
			thread.start();
			try {
				thread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		service.shutdown();
		try{
			if (!service.awaitTermination(120, TimeUnit.SECONDS)) {
				service.shutdownNow();
				if (!service.awaitTermination(120, TimeUnit.SECONDS)){
					log.equals("Service did not shutdown clean");
				}
			}
		}catch(InterruptedException exc){
			service.shutdownNow();
			 Thread.currentThread().interrupt();
		}
		
		try {
			if(writer!=null){
				writer.close();
				writer = null;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(writer != null)
					writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		long endTime = System.currentTimeMillis();
		System.out.println("Total execution time is - "+(endTime - startTime)+" ms...");
	}

}
