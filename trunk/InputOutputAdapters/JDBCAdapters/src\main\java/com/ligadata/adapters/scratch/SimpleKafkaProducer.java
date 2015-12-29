package com.ligadata.adapters.scratch;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;





public class SimpleKafkaProducer {

	public static void main(String []args) {
		// TODO Auto-generated constructor stub
		
		Properties props = new Properties(); 
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.7:9092"); 
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all"); 
        props.put(ProducerConfig.RETRIES_CONFIG, 60); 
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000); 
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1000);
        props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true);
        
        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(props);
        
        boolean sync = false;
		String topic="testTopic";
	
		String key = "mykey";
		String value = "myvalue";
		ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(topic, key, value);
		if (sync) {
			try {
				producer.send(producerRecord).get();
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			producer.send(producerRecord);
		}
		
		System.out.println("Wrote successfully to the topic");
		producer.close();
	}

}
