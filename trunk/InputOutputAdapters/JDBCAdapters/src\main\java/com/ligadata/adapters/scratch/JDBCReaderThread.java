package com.ligadata.adapters.scratch;

import org.easybatch.core.processor.RecordProcessingException;
import org.easybatch.core.reader.RecordReader;
import org.easybatch.core.reader.RecordReaderClosingException;
import org.easybatch.core.reader.RecordReaderOpeningException;
import org.easybatch.core.reader.RecordReadingException;
import org.easybatch.core.record.StringRecord;

import com.google.common.eventbus.EventBus;
import com.ligadata.adapters.processor.StringMapProcessor;
import com.ligadata.adapters.record.JDBCMapRecord;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
public class JDBCReaderThread implements Runnable {
	@Getter @Setter
	private RecordReader recordReader;
	@Getter @Setter
	private EventBus eventbus;
	@Getter @Setter
	private String id;
	@Getter @Setter
	private StringMapProcessor mapProcessor;
	

	@Override
	public void run() {
		try {
			recordReader.open();
			int j=0;
			long startTime = System.currentTimeMillis();
			while(recordReader.hasNextRecord()){
				JDBCMapRecord record = (JDBCMapRecord)recordReader.readNextRecord();
		        StringRecord stringRecord = mapProcessor.processRecord(record);
		        eventbus.post(stringRecord);
		        j++;
			}
			recordReader.close();
			long endTime = System.currentTimeMillis();
			System.out.println(getId()+" took "+(endTime-startTime)+" ms to complete processing "+j+" records...");
		}catch (RecordReaderOpeningException | RecordReadingException | RecordReaderClosingException | RecordProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
