package com.ligadata.adapters.record;

import java.util.Date;

import org.easybatch.core.reader.RecordReader;
import org.easybatch.core.reader.RecordReaderClosingException;
import org.easybatch.core.reader.RecordReaderOpeningException;
import org.easybatch.core.reader.RecordReadingException;
import org.easybatch.core.record.Record;

public interface CustomRecordReader extends RecordReader {
	public void open(Date runDateTime) throws RecordReaderOpeningException ;
}
