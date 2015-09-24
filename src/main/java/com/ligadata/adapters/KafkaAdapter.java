package com.ligadata.adapters;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import sun.misc.Signal;

public class KafkaAdapter extends Thread implements Observer {
	private final AdapterConfiguration configuration;
	private final ConsumerConnector consumer;
	private ExecutorService executor;

	public KafkaAdapter(AdapterConfiguration config) throws Exception {
		this.configuration = config;
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
	}

	@Override
	public void update(Observable o, Object arg) {
		String sig = arg.toString();
		System.out.println("Received signal: " + sig);
		if (sig.compareToIgnoreCase("SIGTERM") == 0 || sig.compareToIgnoreCase("SIGINT") == 0
				|| sig.compareToIgnoreCase("SIGABRT") == 0) {
			System.out.println("Got " + sig + " signal. Shutting down the process");
			shutdown();
			System.exit(0);
		}
	}

	public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();

        try {
            if (!executor.awaitTermination(20000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }

		System.out.println("Shutdown complete.");
	}

	public void run() {

		
    	String topic = configuration.getProperty("kafka.topic");
        int numThreads = Integer.parseInt(configuration.getProperty("kafka.consumer.threads", "2"));
    	
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(numThreads));
        
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
 
        executor = Executors.newFixedThreadPool(numThreads);
 
        int threadNumber = 0;
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new ModelResultProcessor(stream, configuration, threadNumber));
            threadNumber++;
        }
	}

	private ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", configuration.getProperty("zookeeper.connect"));
		props.put("group.id", configuration.getProperty("kafka.group.id"));
		props.put("zookeeper.session.timeout.ms", configuration.getProperty("zookeeper.session.timeout.ms", "400"));
		props.put("zookeeper.sync.time.ms", configuration.getProperty("zookeeper.sync.time.ms", "200"));
		props.put("auto.commit.interval.ms", configuration.getProperty("auto.commit.interval.ms", "1000"));

		return new ConsumerConfig(props);
	}

	public static class AdapterSignalHandler extends Observable implements sun.misc.SignalHandler {

		@Override
		public void handle(Signal sig) {
			setChanged();
			notifyObservers(sig);
		}

		public void handleSignal(String signalName) {
			sun.misc.Signal.handle(new sun.misc.Signal(signalName), this);
		}
	}

	public static void main(String[] args) {

		AdapterConfiguration config = null;
		try {
			if (args.length == 0)
				config = new AdapterConfiguration();
			else if (args.length == 1)
				config = new AdapterConfiguration(args[0]);
			else {
				System.out.println("Incorrect number of arguments. ");
				System.out.println("Usage: KafkaAdapter configfilename");
				System.exit(1);
			}
		} catch (IOException e) {
			System.out.println("Error loading configuration properties.");
			e.printStackTrace();
			System.exit(1);
		}

		KafkaAdapter adapter = null;
		try {
			adapter = new KafkaAdapter(config);
			KafkaAdapter.AdapterSignalHandler sh = new KafkaAdapter.AdapterSignalHandler();
			sh.addObserver(adapter);
			sh.handleSignal("TERM");
			sh.handleSignal("INT");
			sh.handleSignal("ABRT");

			adapter.start();
		} catch (Exception e) {
			System.out.println("Error starting the adapater.\n");
			e.printStackTrace();
			System.exit(1);
		}

		try {
			Thread.sleep(365L * 86400L * 1000L);
		} catch (InterruptedException ie) {
			System.out.println("Main thread is interrupted.\n");
		}
		adapter.shutdown();
	}
}
