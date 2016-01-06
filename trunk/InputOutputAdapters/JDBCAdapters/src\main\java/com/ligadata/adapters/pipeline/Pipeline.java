package com.ligadata.adapters.pipeline;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.easybatch.core.processor.RecordProcessingException;
import org.easybatch.core.reader.RecordReader;
import org.easybatch.core.reader.RecordReaderClosingException;
import org.easybatch.core.reader.RecordReaderOpeningException;
import org.easybatch.core.reader.RecordReadingException;
import org.easybatch.core.record.StringRecord;
import org.json.simple.JSONObject;

import com.google.common.eventbus.AsyncEventBus;
import com.ligadata.adapters.processor.StringMapProcessor;
import com.ligadata.adapters.record.CustomRecordReader;
import com.ligadata.adapters.record.JDBCMapRecord;
import com.ligadata.adapters.utility.SimpleEventListener;
import com.ligadata.adapters.writer.CustomRecordWriter;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
public class Pipeline implements IPipeline{
	//Reader Object for reading records - SOURCE
	@Getter @Setter
	CustomRecordReader reader;
	//Writer Object for writing records - SINK
	@Getter @Setter
	CustomRecordWriter writer;
	//Processor Object to transform Records - TRANSFORMER
	@Getter @Setter
	StringMapProcessor processor;
	//Config Object controlling the configuration
	@Getter @Setter
	JSONObject config;
	
	AsyncEventBus topic;
	SimpleEventListener consumer;
	ExecutorService service;
	
	
	@Override
	public void create(JSONObject configs) {
		// TODO Auto-generated method stub
		config = configs;
		
		JSONObject inputConfig = (JSONObject)config.get("input");
		JSONObject processorConfig = (JSONObject)config.get("processor");
		
		reader = PipelineBuilder.getRecordReader(inputConfig);
		processor = PipelineBuilder.getRecordProcessor(processorConfig);
		
		if(service==null)service = Executors.newFixedThreadPool(4);
		if(topic==null) topic = new AsyncEventBus("JDBC Ingester", service);
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		Date currentDate = new Date();
		
		JSONObject outputConfig = (JSONObject)config.get("output");
		
		if(writer == null)
			writer = PipelineBuilder.getRecordWriter(outputConfig);
		if(consumer == null){
			consumer = new SimpleEventListener(writer);
			topic.register(consumer);
		}
		
		try {
			writer.open();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			reader.open(currentDate);
			int j=0;
			long startTime = System.currentTimeMillis();
			while(reader.hasNextRecord()){
				JDBCMapRecord record = (JDBCMapRecord)reader.readNextRecord();
		        StringRecord stringRecord = processor.processRecord(record);
		        topic.post(stringRecord);
		        j++;
			}
			reader.close();
			long endTime = System.currentTimeMillis();
			System.out.println("Run took "+(endTime-startTime)+" ms to complete processing "+j+" records...");
			
		}catch (RecordReaderOpeningException | RecordReadingException | RecordReaderClosingException | RecordProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		topic.unregister(consumer);
		consumer = null;
		
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
	}
	
	@Override
	public void close() {
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
	}
}
