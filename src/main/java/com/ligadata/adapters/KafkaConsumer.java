package com.ligadata.adapters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class KafkaConsumer implements Runnable {
	private volatile boolean stop = false;

	private final long threadId;
	private final AdapterConfiguration configuration;
	private ConsumerConnector consumer;
	private final BufferedMessageProcessor processor;

	public KafkaConsumer(AdapterConfiguration config) throws Exception {
		this.threadId = Thread.currentThread().getId();
		this.configuration = config;
		String classname = config.getProperty(AdapterConfiguration.MESSAGE_PROCESSOR);
		System.out.println("Thread " + threadId + ": " + " using " + classname + " for processing messages.");
		this.processor = (BufferedMessageProcessor) Class.forName(classname).newInstance();
	}

	public void shutdown() {
		stop = true;
		consumer.shutdown();
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

	@Override
  public void run() {
		System.out.println("Thread " + threadId + ": " + " started processing.");

		long totalMessageCount = 0;
		try {
			consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
			String topic = configuration.getProperty(AdapterConfiguration.KAFKA_TOPIC);

			System.out.println("Thread " + threadId + ": " + " connecting to kafka topic " + topic);
			Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
			topicCountMap.put(topic, new Integer(1));

			Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
			KafkaStream<byte[], byte[]> kafkaStream = consumerMap.get(topic).get(0);

			long syncMessageCount = Long
					.parseLong(configuration.getProperty(AdapterConfiguration.SYNC_MESSAGE_COUNT, "10000"));
			long syncInterval = Long
					.parseLong(configuration.getProperty(AdapterConfiguration.SYNC_INTERVAL_SECONDS, "120")) * 1000;

			ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
			long messageCount = 0;
			long nextSyncTime = System.currentTimeMillis() + syncInterval;
			processor.init(configuration);
			while (!stop) {
				if (hasNext(it)) {
					MessageAndMetadata<byte[], byte[]> t = it.next();
					String message = new String(t.message());
					System.out.println("Thread: " + threadId + ": partition Id :" + t.partition()  + " Message: " + message);
					processor.addMessage(message);
					messageCount++;
				}

				if (messageCount >= syncMessageCount || System.currentTimeMillis() >= nextSyncTime) {
					processor.processAll();
					processor.clearAll();
					consumer.commitOffsets();
					totalMessageCount += messageCount;
					messageCount = 0;
					nextSyncTime = System.currentTimeMillis() + syncInterval;
				}
			}

			consumer.shutdown();

		} catch (Exception e) {
			System.out.println("Error in thread : " + threadId);
			e.printStackTrace();
			e.getCause().printStackTrace();
		}

		System.out.println("Shutting down Thread " + threadId + " after processing " + totalMessageCount + " messages.");
	}
}