package com.ligadata.adapters.record;

import java.util.ArrayList;
import java.util.HashMap;

import org.easybatch.core.record.GenericRecord;
import org.easybatch.core.record.Header;

import lombok.ToString;

@ToString
public class JDBCMapRecord extends GenericRecord<HashMap<String, Object>> {
	
	private static ArrayList<ColumnMetaInfo> fieldTypes; 

	public static ArrayList<ColumnMetaInfo> getFieldTypes() {
		return fieldTypes;
	}

	public static void setFieldTypes(ArrayList<ColumnMetaInfo> fieldTypes) {
		JDBCMapRecord.fieldTypes = fieldTypes;
	}

	public JDBCMapRecord(final Header header, final HashMap payload) {
		super(header, payload);
	}
}
