package com.ligadata.adapters.scratch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.ligadata.adapters.processor.DelimitedRecordProcessor;
import com.ligadata.adapters.record.TableReader;
import com.ligadata.adapters.utility.DBValidator;
import com.ligadata.adapters.utility.SimpleEventListener;
import com.ligadata.adapters.writer.SimpleFileRecordWriter;

import lombok.extern.slf4j.Slf4j;

/*
 * Pipeline for JDBC to File Using CSV Processor
 */
@Slf4j
public class CSVPipeline {
	public static void main(String[] args) {
		String dbUser = "kuser";
		String dbPwd = "centuser";
		String dbName = "kamanja";
		String dbDriver = "org.mariadb.jdbc.Driver";
		String dbURL = "jdbc:mariadb://192.168.1.8:3306";
		//String dbURL = "jdbc:mariadb://10.0.1.105:3306";
		
		String tableName = "testTable";
		String columns = " emp_id, first_name, last_name, application_name, access_time, added_date";
		String whereClause =" where added_date < current_date() ";
		
		int numPartitions=0;
		String partCols = "emp_id";
		
		String trackColumn="added_date";
		boolean runContinuously = true;
		
		
		long startTime = System.currentTimeMillis();
		
		if(DBValidator.validateConnectivity(dbUser, dbPwd, dbDriver, dbURL+"/"+dbName)){
			System.out.println("DB connection successful");
			DataSource ds = DBValidator.getDataSource(dbUser, dbPwd, dbDriver, dbURL+"/"+dbName);
			TableInfo partInfo = new TableInfo();
			partInfo.setDs(ds);
			
			if(DBValidator.validateTable(ds, tableName, columns, whereClause)){
				
				partInfo.setTableName(tableName);
				partInfo.setColumns(columns);
				partInfo.setWhereClause(whereClause);
				
				if(DBValidator.validateTablePartitionCol(ds, tableName, partCols)){
					System.out.println("Validated table partition column....");
					//partInfo.setPartitionColumnName(partCols);
					//partInfo.setPartitions(numPartitions);
					partInfo.fixPartInfo();
					System.out.println(partInfo.toString());
				}else{
					partInfo.fixPartInfo();
					System.out.println(partInfo.toString());
				}
			}
			
			int partCount = partInfo.getPartMap().size();
			
			ExecutorService service = Executors.newFixedThreadPool(4);
			AsyncEventBus eb = new AsyncEventBus("JDBC Ingester", service);
			
			SimpleFileRecordWriter writer = new SimpleFileRecordWriter(null,"test.txt",true,false);
			//SimpleFileRecordWriter writer = new SimpleFileRecordWriter(null,"test.txt",false);
			
			try {
				writer.open();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			SimpleEventListener listener = new SimpleEventListener(writer);
			eb.register(listener);
			
			ArrayList<Thread> threads = new ArrayList<>();
			for(int j=0;j<partCount; j++){
				TableReader reader = new TableReader(ds,tableName,columns);
				if(whereClause != null && whereClause.length() > 0){
					if(partInfo.getPartMap().get(j+1)!=null && partInfo.getPartMap().get(j+1).length()>0)
						reader.setWhereClause(whereClause+" and "+partInfo.getPartMap().get(j+1)+" ");
					else
						reader.setWhereClause(whereClause);
				}else{
					if(partInfo.getPartMap().get(j+1)!=null && partInfo.getPartMap().get(j+1).length()>0)
						reader.setWhereClause(" where "+partInfo.getPartMap().get(j+1)+" ");
					else
						reader.setWhereClause(whereClause);
				}
				reader.setFetchSize(10);
				reader.setQueryTimeout(60);
				
				DelimitedRecordProcessor dRecProcessor = new DelimitedRecordProcessor(":",";",System.getProperty("line.separator"),true);
				Thread thread = new Thread(new JDBCReaderThread(reader,eb, "Thread "+j,dRecProcessor));
				
				threads.add(thread);
			}
			for(int j=0;j<threads.size();j++){
				threads.get(j).start();
			}
			for(int j=0;j<threads.size();j++){
				try {
					threads.get(j).join();
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
}
