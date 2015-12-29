package com.ligadata.adapters.utility;

import org.easybatch.core.processor.RecordProcessingException;
import org.easybatch.core.reader.RecordReaderClosingException;
import org.easybatch.core.reader.RecordReaderOpeningException;
import org.easybatch.core.reader.RecordReadingException;
import org.easybatch.core.record.StringRecord;

import com.google.common.eventbus.EventBus;
import com.ligadata.adapters.processor.StringMapProcessor;
import com.ligadata.adapters.record.JDBCMapRecord;
import com.ligadata.adapters.record.TableReader;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
public class JDBCReaderThread implements Runnable {
	@Getter @Setter
	private TableReader tableReader;
	@Getter @Setter
	private EventBus eventbus;
	@Getter @Setter
	private String id;
	@Getter @Setter
	private StringMapProcessor mapProcessor;
	

	@Override
	public void run() {
		try {
			tableReader.open();
			int j=0;
			long startTime = System.currentTimeMillis();
			while(tableReader.hasNextRecord()){
				JDBCMapRecord record = (JDBCMapRecord)tableReader.readNextRecord();
		        //HashMap<String, Object> employee = record.getPayload();
		        StringRecord stringRecord = mapProcessor.processRecord(record);
		        eventbus.post(stringRecord);
		        j++;
			}
			tableReader.close();
			long endTime = System.currentTimeMillis();
			System.out.println(getId()+" took "+(endTime-startTime)+" to complete processing "+j+" records...");
		}catch (RecordReaderOpeningException | RecordReadingException | RecordReaderClosingException | RecordProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
