package com.ligadata.adapters;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import org.apache.avro.generic.GenericData.Record;

public class PartitionStrategy {
	private class PartitionKey {
		String attribute;
		DateFormat format;
		
		private PartitionKey(String attribute, String formatStr) {
			this.attribute = attribute;
			this.format = new SimpleDateFormat(formatStr);
		}

		private PartitionKey(String attribute) {
			this.attribute = attribute;
			this.format = null;
		}
	}
	
	private ArrayList<PartitionKey> keys = null;
	
	public PartitionStrategy() {
	}
	
	public PartitionStrategy(String format) {
		if(format == null || "".equals(format))
			return;
		
		keys = new ArrayList<PartitionKey>();
		
		for(String token: format.split(",")) {
			String[] attr = token.split(":");
			PartitionKey key;
			if(attr.length > 1) 
				key = new PartitionKey(attr[0], attr[1]);
			else
				key= new PartitionKey(attr[0]);
			
			keys.add(key);
		}
	}

	public String getPartition(Record rec) {
		String partition = "";
		SimpleDateFormat input = new SimpleDateFormat("yyyy-MM-dd");
		if(keys != null) {
			for (PartitionKey key : keys) {
				String value = "";
				if(key.format == null)
					value = rec.get(key.attribute).toString();
				else {
					try {
						Date dateValue = input.parse(rec.get(key.attribute).toString());
						value = key.format.format(dateValue);
					} catch (ParseException e) {
					}
				}
				if(value != null)
					partition = partition + "/" + value;
			}
			partition = partition.substring(1);
		}
		
		return partition;
	}

}
