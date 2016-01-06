package com.ligadata.adapters.utility;

import org.easybatch.core.record.StringRecord;
import org.easybatch.core.writer.RecordWritingException;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import com.ligadata.adapters.writer.CustomRecordWriter;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
public class SimpleEventListener {
	@Getter @Setter
	CustomRecordWriter writer;
	
	@Subscribe 
	@AllowConcurrentEvents
	public void task(StringRecord rec){
		//System.out.println("Recevied "+s+" @ "+new Date());
		try {
			writer.write(rec);
		} catch (RecordWritingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
