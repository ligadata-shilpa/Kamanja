package com.ligadata.adapters.writer;

import java.io.IOException;
import java.util.Collection;

import org.easybatch.core.record.StringRecord;
import org.easybatch.core.writer.RecordWritingException;

public interface CustomRecordWriter {
	void open() throws IOException;
	void write(StringRecord record) throws RecordWritingException;
	void writeBatch(Collection<StringRecord> records) throws RecordWritingException;
	void close() throws IOException;
}
