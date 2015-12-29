package com.ligadata.adapters.processor;

import org.easybatch.core.processor.RecordProcessingException;
import org.easybatch.core.record.StringRecord;

import com.google.gson.JsonObject;
import com.ligadata.adapters.record.ColumnMetaInfo;
import com.ligadata.adapters.record.JDBCMapRecord;


public class JSONRecordProcessor implements StringMapProcessor {
	public StringRecord processRecord(JDBCMapRecord record) throws RecordProcessingException {
		JsonObject jsonObject = new JsonObject();
		for(ColumnMetaInfo info:JDBCMapRecord.getFieldTypes()){
			jsonObject.addProperty(info.getColumnName(), record.getPayload().get(info.getColumnName()).toString());
		}
		return new StringRecord(record.getHeader(), jsonObject.toString());
	}
}
