package com.ligadata.adapters.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AvroHDFSWriter {
	
	private Schema schema;
	private String basePath;
	private String codec;
	private URI uri;
	private FSDataOutputStream out;
	private DataFileWriter<Record> dataFileWriter;

	public AvroHDFSWriter(Schema schema, String path, String codec) throws Exception {
		this.schema = schema;
		this.basePath = path;
		this.codec = codec;
	}
	
	public Schema getSchema() {
		return schema;
	}
	
	public void open(String fileName) throws IOException {
		uri = URI.create(basePath + "/" + fileName);
		System.out.println("Thread " + Thread.currentThread().getId() + ": Opening avro writer for " + uri);
		
		Configuration conf = new Configuration();
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		FileSystem fs = FileSystem.get(uri, conf);

		DatumWriter<Record> datumWriter = new GenericDatumWriter<Record>(schema);
		dataFileWriter = new DataFileWriter<Record>(datumWriter);
		if(codec == null || "".equals(codec))
			dataFileWriter.setCodec(CodecFactory.fromString("null"));
		else
			dataFileWriter.setCodec(CodecFactory.fromString(codec));
		
		Path path = new Path(uri);
		if (fs.exists(path)) {
			System.out.println("Thread " + Thread.currentThread().getId() + ": Loading existing file " + uri);
			out = fs.append(path);
			dataFileWriter.appendTo(new FsInput(path, conf), out);
		} else {
			System.out.println("Thread " + Thread.currentThread().getId() + ": Creating new file " + uri);
			out = fs.create(path);
			dataFileWriter.create(schema, out);
		}
	}
	
	public void write(Record rec) throws IOException {
		dataFileWriter.append(rec);
	}

	public void close() throws IOException {
		System.out.println("Thread " + Thread.currentThread().getId() + ": Closing file at " + uri);
		if (dataFileWriter != null)
			dataFileWriter.close();
		if (out != null)
			out.close();
		
		dataFileWriter = null;
		out = null;
		uri = null;
	}
}
