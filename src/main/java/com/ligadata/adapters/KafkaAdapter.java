package com.ligadata.adapters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import sun.misc.Signal;

@SuppressWarnings("restriction")
public class KafkaAdapter implements Observer {
	private AdapterConfiguration configuration;
	private ArrayList<KafkaConsumer> consumers;
	private ExecutorService executor;

	public KafkaAdapter(AdapterConfiguration config) {
		this.configuration = config;
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
		for(KafkaConsumer c: consumers)
			c.shutdown();
		
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(30000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }

		System.out.println("Shutdown complete.");
	}

	public void run() {
        int numThreads = Integer.parseInt(configuration.getProperty(AdapterConfiguration.COUNSUMER_THREADS, "2"));
        executor = Executors.newFixedThreadPool(numThreads);
        consumers = new ArrayList<KafkaConsumer>();
        for (int threadNumber = 0; threadNumber < numThreads; threadNumber++) {
        	KafkaConsumer c = new KafkaConsumer(configuration, threadNumber);
            executor.submit(c);
        	consumers.add(c);
        }
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
				System.out.println("Usage: KafkaAdapter [configfilename]");
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

			adapter.run();
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
