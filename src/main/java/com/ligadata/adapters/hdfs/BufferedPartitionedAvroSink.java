package com.ligadata.adapters.hdfs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import com.ligadata.adapters.AdapterConfiguration;
import com.ligadata.adapters.BufferedMessageProcessor;

public class BufferedPartitionedAvroSink implements BufferedMessageProcessor {
	private String name;
	private PartitionStrategy partitioner;
	private Map<String, ArrayList<Record>> buffer;
	private AvroHDFSWriter hdfsWriter;
	private Schema schema;
	
	public BufferedPartitionedAvroSink() {
	}
		
	private Record json2Record(String jsonStr) throws IOException {
		DatumReader<Record> reader = new GenericDatumReader<Record>(schema);
		Decoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonStr);
		return reader.read(null, decoder);
	}

	private void addRecord(Record rec) {
		String partitionKey = partitioner.getPartition(rec);
		ArrayList<Record> partition = buffer.get(partitionKey);
		if(partition == null) {
			partition = new ArrayList<Record>();
			buffer.put(partitionKey, partition);
		}
		
		partition.add(rec);
	}
	
	@Override
	public void init(AdapterConfiguration configuration) throws Exception {
		this.name = configuration.getProperty(AdapterConfiguration.FILE_PREFIX, "Log") + Thread.currentThread().getId();
		this.buffer = new HashMap<String, ArrayList<Record>>();
		this.partitioner = new PartitionStrategy(configuration.getProperty(AdapterConfiguration.PARTITION_STRATEGY),
				configuration.getProperty(AdapterConfiguration.INPUT_DATE_FORMAT, "yyyy-MM-dd"));
		
		String schemaFile = configuration.getProperty(AdapterConfiguration.SCHEMA_FILE, "InstrumentationLog.avsc");
		System.out.println("Thread " + Thread.currentThread().getId() + ": Using avro schema from file: " + schemaFile);
		this.schema = new Schema.Parser().parse(new File(schemaFile));

		this.hdfsWriter = new AvroHDFSWriter(schema, 
				configuration.getProperty(AdapterConfiguration.HDFS_URI),
				configuration.getProperty(AdapterConfiguration.FILE_COMPRESSION));
		this.hdfsWriter.setKeytabFileKey(configuration.getProperty(AdapterConfiguration.HDFS_KEYTABFILE_KEY));
		this.hdfsWriter.setUserNameKey(configuration.getProperty(AdapterConfiguration.HDFS_USERNAME_KEY));

	}

	@Override
	public void addMessage(String message) {
		Record record = null;
		try {
			record = json2Record(message);
		} catch (Exception e) {
			System.out.println("Thread " + Thread.currentThread().getId() + ": Error parsing message: " + e.getMessage());
			e.printStackTrace();
		}
		if (record != null)
			addRecord(record);		
	}

	@Override
	public void processAll() throws Exception {
		for (String key : buffer.keySet()) {
			try {
				System.out.println("Thread " + Thread.currentThread().getId() + ": Writing partition [" + key + "]");
				String filename = name + ".avro";
				if(key != null && !key.equalsIgnoreCase(""))
					filename = key + "/" + filename;
				hdfsWriter.open(filename);
				ArrayList<Record> records = buffer.get(key);
				for (Record rec : records) {
					hdfsWriter.write(rec);
				}
			} finally {
				hdfsWriter.close();
			}
		}		
	}

	@Override
	public void clearAll() {
		buffer.clear();			
	}

	@Override
	public void close() {
	}
}
