package com.ligadata.adapters;

import java.io.IOException;
import org.apache.avro.generic.GenericData.Record;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class ModelResultProcessor implements Runnable {
	private KafkaStream<byte[], byte[]> m_stream;
	private int m_threadNumber;
	private AdapterConfiguration configuration;

	public ModelResultProcessor(KafkaStream<byte[], byte[]> a_stream, AdapterConfiguration config, int a_threadNumber) {
		m_threadNumber = a_threadNumber;
		m_stream = a_stream;
		this.configuration = config;
	}

	public void run() {
		System.out.println("Thread " + m_threadNumber + ": " + " Opening dataset writer.");

		int messageCount = 0;
		AvroHDFSWriter hdfsWriter = null;
		try {
			String destFile = configuration.getProperty("dataset.hdfs.uri") + "/Log" + m_threadNumber + ".avro";
			String schemaFile = configuration.getProperty("dataset.schema.file", "InstrumentationLog.avsc");
			hdfsWriter = new AvroHDFSWriter(schemaFile, destFile);

			ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
			while (it.hasNext()) {
				String message = new String(it.next().message());
				System.out.println("Thread " + m_threadNumber + ": " + message);
				Record record = null;
				try {
					record = hdfsWriter.json2Record(message);
				} catch (Exception e) {
					System.out.println("Error parsing message: " + e.getMessage());
					e.printStackTrace();
				}
				if (record != null)
					hdfsWriter.append(record);
				messageCount++;
			}

		} catch (Exception e) {
			System.out.println("Error in thread : " + m_threadNumber);
			e.printStackTrace();
			e.getCause().printStackTrace();
		} finally {
			System.out.println("Thread " + m_threadNumber + ": " + " Closing dataset writer after processing " + messageCount + " messages.");
			if (hdfsWriter != null)
				try {
					hdfsWriter.close();
				} catch (IOException e) {
					System.out.println("Error closing writer in thread : " + m_threadNumber);
					e.printStackTrace();
				}			
		}

		System.out.println("Shutting down Thread: " + m_threadNumber);
	}
}