package com.ligadata.adapters.processor;

import org.easybatch.core.processor.RecordProcessingException;
import org.easybatch.core.processor.RecordProcessor;
import org.easybatch.core.record.StringRecord;

import com.ligadata.adapters.record.JDBCMapRecord;

public interface StringMapProcessor extends RecordProcessor<JDBCMapRecord, StringRecord> {
	StringRecord processRecord(JDBCMapRecord record) throws RecordProcessingException;
}
