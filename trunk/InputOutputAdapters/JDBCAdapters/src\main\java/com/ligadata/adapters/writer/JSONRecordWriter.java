package com.ligadata.adapters.writer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.zip.GZIPOutputStream;

import org.easybatch.core.record.StringRecord;
import org.easybatch.core.writer.RecordWritingException;

import com.google.gson.stream.JsonWriter;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@ToString
public class JSONRecordWriter implements CustomRecordWriter{
	@Getter @Setter
	String fileName;
	@Getter @Setter
	JsonWriter writer;
	@Getter @Setter
	boolean zipOutput;

	@Override
	public void open() throws IOException {
		File f = null;
		if(zipOutput)
			f = new File(fileName+".gz");
		else
			f = new File(fileName);
		if(!f.exists())
			f.createNewFile();
		
		if(zipOutput)
			writer = new JsonWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(f)),"UTF-8"));
		else
			writer = new JsonWriter(new OutputStreamWriter(new FileOutputStream(f),"UTF-8"));
		
		writer.beginArray();
	}

	@Override
	public void write(StringRecord record) throws RecordWritingException {
		try {
			writer.jsonValue(record.getPayload());
			writer.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RecordWritingException("Unable to write record",e);
		}
	}

	@Override
	public void writeBatch(Collection<StringRecord> records) throws RecordWritingException {
		try{
			for(StringRecord rec:records){
				writer.jsonValue(rec.getPayload());
			}
			writer.flush();
		}catch(IOException e){
			e.printStackTrace();
			throw new RecordWritingException("Unable to write record",e);
		}
	}

	@Override
	public void close() throws IOException {
		writer.endArray();
		writer.flush();
		writer.close();
	}


}
