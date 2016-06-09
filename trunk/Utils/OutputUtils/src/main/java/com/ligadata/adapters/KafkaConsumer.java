package com.ligadata.adapters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class KafkaConsumer implements Runnable {
	static Logger logger = Logger.getLogger(KafkaConsumer.class);
	private volatile boolean stop = false;
	private Thread thisThread = null;

	private final AdapterConfiguration configuration;
	private ConsumerConnector consumer;
	private final BufferedMessageProcessor processor;
	private HashMap<Integer, Long> partitionOffsets = new HashMap<Integer, Long>();

	public KafkaConsumer(AdapterConfiguration config) throws Exception {
		this.configuration = config;
		String classname = configuration.getProperty(AdapterConfiguration.MESSAGE_PROCESSOR);
		if(classname == null || "".equals(classname) || "null".equalsIgnoreCase(classname)) {
			logger.info("Message prcessor not specified for processing messages.");
			processor = new NullProcessor();
		} else {
			logger.info("Loading class " + classname + " for processing messages.");
			processor = (BufferedMessageProcessor) Class.forName(classname).newInstance();
		}
	}

	public void shutdown() {
		stop = true;
		if(processor != null)
			processor.close();
		if(consumer != null)
			consumer.shutdown();
		if(thisThread != null)
			thisThread.interrupt();
	}

	private ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", configuration.getProperty(AdapterConfiguration.ZOOKEEPER_CONNECT));
		props.put("zookeeper.session.timeout.ms",
				configuration.getProperty(AdapterConfiguration.ZOOKEEPER_SESSION_TIMEOUT, "400"));
		props.put("zookeeper.sync.time.ms", configuration.getProperty(AdapterConfiguration.ZOOKEEPER_SYNC_TIME, "200"));
		
		props.put("group.id", configuration.getProperty(AdapterConfiguration.KAFKA_GROUP_ID));
		//props.put("auto.offset.reset", configuration.getProperty(AdapterConfiguration.KAFKA_AUTO_OFFSET_RESET, "largest"));
		//props.put("offsets.storage", configuration.getProperty(AdapterConfiguration.KAFKA_OFFSETS_STORAGE, "zookeeper"));
		props.put("consumer.timeout.ms", "1000");
		
		// Add any additional properties specified for Kafka
		for(String key: configuration.getProperties().stringPropertyNames()) {
			if(key.startsWith(AdapterConfiguration.KAFKA_PROPERTY_PREFIX)) {
				logger.debug("Adding kafka configuration: " + key + "=" + configuration.getProperty(key));
				props.put(key.substring(AdapterConfiguration.KAFKA_PROPERTY_PREFIX.length()), configuration.getProperty(key));
			}
		}
		
		props.put("dual.commit.enabled", "false");
		props.put("auto.commit.enable", "false");

		return new ConsumerConfig(props);
	}

	private boolean hasNext(ConsumerIterator<byte[], byte[]> it) {
		try {
			return it.hasNext();
		} catch (ConsumerTimeoutException e) {
			return false;
		}
	}

	private KafkaStream<byte[], byte[]> getKafkaStream() {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
		String topic = configuration.getProperty(AdapterConfiguration.KAFKA_TOPIC);

		logger.info("Connecting to kafka topic " + topic);
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		return consumerMap.get(topic).get(0);
	}
	
	private KafkaStream<byte[], byte[]> getKafkaStreamWithRetry() {
		long retry = 0;
		long retryInterval = 5000;
		while (!stop) {
			try {
				KafkaStream<byte[], byte[]> stream = getKafkaStream();
				if(retry > 0)
					logger.info("Successfully connected after " + retry + " retries.");
				return stream;
			} catch (Exception e) {
				retry++;
				if(retry <= 12) {
					logger.error("Error after " + retry + " retries : " + e.getMessage(), e);
					try { Thread.sleep(retryInterval*retry); } catch (InterruptedException e1) {}
				} else
					try { Thread.sleep(60000); } catch (InterruptedException e1) {}
			}
		}
		
		return null;
	}

	private void processWithRetry() {
		long retry = 0;
		long retryInterval = 5000;
		while (!stop) {
			try {
				processor.processAll();
				if(retry > 0)
					logger.info("Successfully processed messages after " + retry + " retries.");
				return;
			} catch (Exception e) {
				retry++;
				if(retry <= 12) {
					logger.error("Error after " + retry + " retries : " + e.getMessage(), e);
					try { Thread.sleep(retryInterval*retry); } catch (InterruptedException e1) {}
				} else
					try { Thread.sleep(60000); } catch (InterruptedException e1) {}
			}
		}
	}
	
	@Override
	public void run() {
		logger.info("Kafka consumer started processing.");
		thisThread = Thread.currentThread();

		long totalMessageCount = 0;
		long errorMessageCount = 0;
		try {

			logger.info("Using " + processor.getClass().getName() + " for processing messages.");
			processor.init(configuration);

		} catch (Exception e) {
			logger.error("Error initializing processor: " + e.getMessage(), e);
			throw new RuntimeException(e);
		}

		KafkaStream<byte[], byte[]> kafkaStream = getKafkaStreamWithRetry();

		long syncMessageCount = Long.parseLong(configuration.getProperty(AdapterConfiguration.SYNC_MESSAGE_COUNT, "10000"));
		long syncInterval = Long.parseLong(configuration.getProperty(AdapterConfiguration.SYNC_INTERVAL_SECONDS, "120")) * 1000;

		ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
		long messageCount = 0;
		long nextSyncTime = System.currentTimeMillis() + syncInterval;
		long start = System.currentTimeMillis();
		while (!stop) {
			try {
				if (hasNext(it)) {
					MessageAndMetadata<byte[], byte[]> t = it.next();
					Long lastOffset = partitionOffsets.get(t.partition());
					if (lastOffset == null || t.offset() > lastOffset) {
						String message = new String(t.message());
						logger.debug("Message from partition Id :" + t.partition() + " Message: " + message);
						if (processor.addMessage(message))
							messageCount++;
						else
							errorMessageCount++;

						partitionOffsets.put(t.partition(), t.offset());
					}
				}
			} catch (Exception e) {
				logger.error("Error reading from kafka: " + e.getMessage(), e);
				KafkaStream<byte[], byte[]> stream = getKafkaStreamWithRetry();
				it = stream.iterator();
			}

			if (messageCount > 0 && (messageCount >= syncMessageCount || System.currentTimeMillis() >= nextSyncTime)) {
				long endRead = System.currentTimeMillis();
				logger.info("Saving " + messageCount + " messages. Read time " + (endRead - start) + " msecs.");
				processWithRetry();
				processor.clearAll();
				long endWrite = System.currentTimeMillis();
				consumer.commitOffsets();
				totalMessageCount += messageCount;
				logger.info("Saved " + messageCount + " messages. Write time " + (endWrite - endRead) + " msecs.");

				messageCount = 0;
				nextSyncTime = System.currentTimeMillis() + syncInterval;
				start = System.currentTimeMillis();
			}
		}

		consumer.shutdown();

		logger.info("Shutting down after processing " + totalMessageCount + " messages with " + errorMessageCount
				+ " error messages.");
	}
}