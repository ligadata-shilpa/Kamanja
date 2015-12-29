package com.ligadata.adapters.pipeline;

import java.io.IOException;
import java.util.ArrayList;

import javax.sql.DataSource;

import com.google.common.eventbus.EventBus;
import com.ligadata.adapters.pojo.TablePartitionInfo;
import com.ligadata.adapters.processor.DelimitedRecordProcessor;
import com.ligadata.adapters.record.TableReader;
import com.ligadata.adapters.utility.DBValidator;
import com.ligadata.adapters.utility.JDBCReaderThread;
import com.ligadata.adapters.utility.SimpleEventListener;
import com.ligadata.adapters.writer.KafkaRecordWriter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ManualPipeline {

	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String dbUser = "kuser";
		String dbPwd = "centuser";
		String dbName = "kamanja";
		String dbDriver = "org.mariadb.jdbc.Driver";
		//String dbURL = "jdbc:mariadb://192.168.1.7:3306";
		String dbURL = "jdbc:mariadb://10.0.1.105:3306";
		String tableName = "testTable";
		String columns = " emp_id, first_name, last_name, application_name, access_time, added_date";
		String whereClause =" where added_date < current_date() ";
		
		int numPartitions=0;
		String partCols = "emp_id";
		
		
		if(DBValidator.validateConnectivity(dbUser, dbPwd, dbDriver, dbURL+"/"+dbName)){
			System.out.println("DB connection successful");
			DataSource ds = DBValidator.getDataSource(dbUser, dbPwd, dbDriver, dbURL+"/"+dbName);
			TablePartitionInfo partInfo = new TablePartitionInfo();
			
			partInfo.setDs(ds);
			
			if(DBValidator.validateTable(ds, tableName, columns, whereClause)){
				
				partInfo.setTableName(tableName);
				partInfo.setColumns(columns);
				partInfo.setWhereClause(whereClause);
				
				if(DBValidator.validateTablePartitionCol(ds, tableName, partCols)){
					System.out.println("Validated table partition column....");
					partInfo.setPartitionColumnName(partCols);
					partInfo.setPartitions(numPartitions);
					partInfo.fixPartInfo();
					System.out.println(partInfo.toString());
				}
			}
			
			int partCount = partInfo.getPartMap().size();
			
			/*
			ExecutorService service = null;
			if(partCount > 0 )
				service = Executors.newFixedThreadPool(1);
			else
				service = Executors.newFixedThreadPool(partCount);
			AsyncEventBus eb = new AsyncEventBus("JDBC Ingester", service);
			*/
			
			EventBus eb = new EventBus("JDBC Ingester");
			
			//SimpleFileRecordWriter writer = new SimpleFileRecordWriter(null,"test.txt",true);
			//SimpleFileRecordWriter writer = new SimpleFileRecordWriter(null,"test.txt",false);
			
			//JSONRecordWriter writer = new JSONRecordWriter("test.json",null,true);
			
			KafkaRecordWriter writer = new KafkaRecordWriter();
			
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
				if(whereClause != null && whereClause.length() > 0)
					reader.setWhereClause(whereClause+" and "+partInfo.getPartMap().get(j+1)+" ");
				else
					reader.setWhereClause(" where "+partInfo.getPartMap().get(j+1)+" ");
				reader.setFetchSize(10);
				reader.setQueryTimeout(60);
				
				DelimitedRecordProcessor dRecProcessor = new DelimitedRecordProcessor(":",";",System.getProperty("line.separator"),true);
				Thread thread = new Thread(new JDBCReaderThread(reader,eb, "Thread "+j,dRecProcessor));
				
				//JSONRecordProcessor jsonProcessor = new JSONRecordProcessor();
				//Thread thread = new Thread(new JDBCReaderThread(reader, eb, "Thread "+j,jsonProcessor));
				
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
			
			/*
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
			*/
		}
	}
}
