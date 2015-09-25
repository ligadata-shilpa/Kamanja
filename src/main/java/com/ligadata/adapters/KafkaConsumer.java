package com.ligadata.adapters;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer implements Runnable {
	private volatile boolean stop = false;
	
	private int threadNumber;
	private AdapterConfiguration configuration;
	private Schema schema;
	private ConsumerConnector consumer;

	public KafkaConsumer(AdapterConfiguration config, int threadNumber) {
		this.threadNumber = threadNumber;
		this.configuration = config;
	}
	
	public void shutdown() {
		stop = true;
		consumer.shutdown();
	}

	private Record json2Record(String jsonStr) throws IOException {
		DatumReader<Record> reader = new GenericDatumReader<Record>(schema);
		Decoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonStr);
		return reader.read(null, decoder);
	}

	private ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", configuration.getProperty(AdapterConfiguration.ZOOKEEPER_CONNECT));
		props.put("zookeeper.session.timeout.ms",
				configuration.getProperty(AdapterConfiguration.ZOOKEEPER_SESSION_TIMEOUT, "400"));
		props.put("zookeeper.sync.time.ms", configuration.getProperty(AdapterConfiguration.ZOOKEEPER_SYNC_TIME, "200"));
		props.put("group.id", configuration.getProperty(AdapterConfiguration.KAFKA_GROUP_ID));
		props.put("auto.commit.enable", "false");
		props.put("consumer.timeout.ms", "1000");

		return new ConsumerConfig(props);
	}

	private boolean hasNext(ConsumerIterator<byte[], byte[]> it) {
		try {
			return it.hasNext();
		} catch (ConsumerTimeoutException e) {
			return false;
		}
	}

	public void run() {
		System.out.println("Thread " + threadNumber + ": " + " started processing.");

		long messageCount = 0;
		AvroHDFSWriter hdfsWriter = null;
		try {
			consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
			String topic = configuration.getProperty(AdapterConfiguration.KAFKA_TOPIC);

			System.out.println("Thread " + threadNumber + ": " + " connecting to kafka topic " + topic);
			Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
			topicCountMap.put(topic, new Integer(1));

			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
			KafkaStream<byte[], byte[]> kafkaStream = consumerMap.get(topic).get(0);
			
			PartitionStrategy strategy = new PartitionStrategy(configuration.getProperty(AdapterConfiguration.PARTITION_STRATEGY));

			PartitionedAvroBuffer buffer = new PartitionedAvroBuffer(
					configuration.getProperty(AdapterConfiguration.FILE_PREFIX, "Log") + threadNumber);
			buffer.setPartitionStrategy(strategy);

			String schemaFile = configuration.getProperty(AdapterConfiguration.SCHEMA_FILE, "InstrumentationLog.avsc");
			this.schema = new Schema.Parser().parse(new File(schemaFile));

			hdfsWriter = new AvroHDFSWriter(schema, 
					configuration.getProperty(AdapterConfiguration.HDFS_URI),
					configuration.getProperty(AdapterConfiguration.FILE_COMPRESSION));
			
			long syncMessageCount = Long
					.parseLong(configuration.getProperty(AdapterConfiguration.SYNC_MESSAGE_COUNT, "10000"));
			long syncInterval = Long
					.parseLong(configuration.getProperty(AdapterConfiguration.SYNC_INTERVAL_SECONDS, "120")) * 1000;

			ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
			long nextSyncTime = System.currentTimeMillis() + syncInterval;
			while (!stop) {
				if (hasNext(it)) {
					String message = new String(it.next().message());
					System.out.println("Thread " + threadNumber + ": " + message);
					Record record = null;
					try {
						record = json2Record(message);
					} catch (Exception e) {
						System.out.println("Error parsing message: " + e.getMessage());
						e.printStackTrace();
					}
					if (record != null)
						buffer.addRecord(record);

					if (buffer.getSize() >= syncMessageCount || System.currentTimeMillis() >= nextSyncTime) {
						buffer.write(hdfsWriter);
						buffer.clear();
						consumer.commitOffsets();
						nextSyncTime = System.currentTimeMillis() + syncInterval;
					}
					messageCount++;
				}
			}

			consumer.shutdown();

		} catch (Exception e) {
			System.out.println("Error in thread : " + threadNumber);
			e.printStackTrace();
			e.getCause().printStackTrace();
		}

		System.out.println("Shutting down Thread " + threadNumber + " after processing " + messageCount + " messages.");
	}
}