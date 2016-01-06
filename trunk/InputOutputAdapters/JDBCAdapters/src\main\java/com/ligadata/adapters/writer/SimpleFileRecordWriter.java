package com.ligadata.adapters.writer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.zip.GZIPOutputStream;

import org.easybatch.core.record.StringRecord;
import org.easybatch.core.writer.RecordWritingException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
public class SimpleFileRecordWriter implements CustomRecordWriter{
	@Getter @Setter 
	private Writer writer;
	@Getter @Setter
	private String fileName;
	@Getter @Setter
	private boolean zipOutput;
	@Getter @Setter
	private boolean append;
	
	public void open() throws IOException{
		File f = null;
		if(zipOutput)
			f = new File(fileName+".gz");
		else
			f = new File(fileName);
		
		if(!f.exists())
			f.createNewFile();
		
		FileOutputStream fos = null;
		if(append)
			fos = new FileOutputStream(f, append);
		else 
			fos = new FileOutputStream(f);
		
		if(zipOutput)
			writer = new OutputStreamWriter(new GZIPOutputStream(fos),"UTF-8");
		else
			writer = new OutputStreamWriter(fos,"UTF-8");
	}
	
	public void close() throws IOException{
		writer.flush();
		writer.close();
	}
	
	public void write(StringRecord record) throws RecordWritingException{
		try {
			writer.write(record.getPayload());
			writer.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RecordWritingException("Unable to write Record",e);
		}
	}
	
	public void writeBatch(Collection<StringRecord> records) throws RecordWritingException{
		try{
			for(StringRecord record:records){
				writer.write(record.getPayload());
			}
			writer.flush();
		}catch(IOException exc){
			exc.printStackTrace();
			throw new RecordWritingException("Unable to write Record",exc);
		}
	}
}
