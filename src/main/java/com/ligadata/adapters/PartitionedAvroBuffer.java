package com.ligadata.adapters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericData.Record;

public class PartitionedAvroBuffer {
	private String name;
	private long size;
	private PartitionStrategy partitioner;
	private Map<String, ArrayList<Record>> buffer;
	
	public PartitionedAvroBuffer(String name) {
		this.name = name;
		this.size = 0;
		buffer = new HashMap<String, ArrayList<Record>>();
		partitioner = new PartitionStrategy();
	}
	
	public String getName() {
		return name;
	}
	
	public PartitionStrategy getPartitioner() {
		return partitioner;
	}

	public void setPartitioner(PartitionStrategy partitioner) {
		this.partitioner = partitioner;
	}

	public long getSize() {
		return size;
	}
	
	public void addRecord(Record rec) {
		String partitionKey = partitioner.getPartition(rec);
		ArrayList<Record> partition = buffer.get(partitionKey);
		if(partition == null) {
			partition = new ArrayList<Record>();
			buffer.put(partitionKey, partition);
		}
		
		partition.add(rec);
		this.size++;
	}
	
	public void clear() {
		buffer.clear();	
	}
	
	public void write(AvroHDFSWriter writer) throws IOException {
		for (String key : buffer.keySet()) {
			try {
				System.out.println("Writing partition [" + key + "]");
				String filename = name + ".avro";
				if(key != null && !key.equalsIgnoreCase(""))
					filename = key + "/" + filename;
				writer.open(filename);
				ArrayList<Record> records = buffer.get(key);
				for (Record rec : records) {
					writer.write(rec);
				}
			} finally {
				writer.close();
			}
		}
	}

}
