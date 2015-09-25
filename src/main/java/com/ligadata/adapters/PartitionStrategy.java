package com.ligadata.adapters;

import org.apache.avro.generic.GenericData.Record;

public class PartitionStrategy {

	public String getPartition(Record rec) {
		return "";
	}

}
