package com.ligadata.adapters;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AvroHDFSWriter {
	
	private Schema schema;
	FSDataOutputStream out;
	DataFileWriter<Record> dataFileWriter;

	public AvroHDFSWriter(String schemaFile, String destinationFile) throws Exception {
		this.schema = new Schema.Parser().parse(new File(schemaFile));
		URI uri = URI.create(destinationFile);
		System.out.println("Opening dataset writer." + uri);
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(uri, conf);

		DatumWriter<Record> datumWriter = new GenericDatumWriter<Record>(schema);
		dataFileWriter = new DataFileWriter<Record>(datumWriter);

		Path path = new Path(uri);
		if (fs.exists(path)) {
			System.out.println("Loading existing dataset at " + uri);
			out = fs.append(path);
			dataFileWriter.appendTo(new FsInput(path, conf), out);
		} else {
			System.out.println("Creating new dataset at " + uri);
			out = fs.create(path);
			dataFileWriter.create(schema, out);
		}
	}
	
	public Record json2Record(String jsonStr) throws IOException {
			DatumReader<Record> reader = new GenericDatumReader<Record>(schema);
			Decoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonStr);
			return reader.read(null, decoder);
	}
			
	public Schema getSchema() {
		return schema;
	}
	
	public void append(Record rec) throws IOException {
		dataFileWriter.append(rec);
	}

	public void close() throws IOException {
		if (dataFileWriter != null)
			dataFileWriter.close();
		if (out != null)
			out.close();
	}

}
