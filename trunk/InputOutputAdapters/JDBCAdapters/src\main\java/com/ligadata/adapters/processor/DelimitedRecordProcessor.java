package com.ligadata.adapters.processor;

import org.easybatch.core.processor.RecordProcessingException;
import org.easybatch.core.record.StringRecord;

import com.ligadata.adapters.record.ColumnMetaInfo;
import com.ligadata.adapters.record.JDBCMapRecord;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor
@NoArgsConstructor
@ToString
public class DelimitedRecordProcessor implements StringMapProcessor {
	@Getter @Setter
	String keyValueDelimiter;
	@Getter @Setter
	String columnDelimiter;
	@Getter @Setter
	String rowDelimiter;
	@Getter @Setter
	boolean addKeys=false;
	
	
	@Override
	public StringRecord processRecord(JDBCMapRecord record) throws RecordProcessingException {
		StringBuilder builder = new StringBuilder();
		
		if(getKeyValueDelimiter()!=null && getKeyValueDelimiter().length()>0);
		else
			throw new RecordProcessingException("Key Value Delimiter cannot be null");
		if(getColumnDelimiter()!=null && getColumnDelimiter().length()>0);
		else
			throw new RecordProcessingException("Column Delimiter cannot be null");
		if(getRowDelimiter()!=null && getRowDelimiter().length()>0);
		else
			throw new RecordProcessingException("Row Delimiter cannot be null");
		
		int j=0;
		for(ColumnMetaInfo info:JDBCMapRecord.getFieldTypes()){
			j++;
			if(isAddKeys())
				builder.append(info.getColumnName()+getKeyValueDelimiter()+record.getPayload().get(info.getColumnName()));
			else
				builder.append(record.getPayload().get(info.getColumnName()));
			if(j == JDBCMapRecord.getFieldTypes().size()){
				builder.append(getRowDelimiter());
			}else{
				builder.append(getColumnDelimiter());
			}
		}
		return new StringRecord(record.getHeader(), builder.toString());
	}

}
