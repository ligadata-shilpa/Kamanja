package com.ligadata.adapters.writer;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.easybatch.core.record.StringRecord;
import org.easybatch.core.writer.RecordWritingException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class KafkaRecordWriter implements CustomRecordWriter{
	@Getter @Setter
	Properties producerProperties;
	@Getter @Setter
	KafkaProducer<String,String> producer;
	@Getter @Setter
	String topic;
	@Getter @Setter
	boolean sync;
	
	@Override
	public void open() throws IOException {
		producerProperties = new Properties();
		producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.7:9092"); 
		producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProperties.put(ProducerConfig.ACKS_CONFIG, "all"); 
		producerProperties.put(ProducerConfig.RETRIES_CONFIG, 60); 
		producerProperties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000); 
		producerProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
		producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1000);
		producerProperties.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true);
		
		sync = false;
		
		topic = "testTopic";
		
		producer = new KafkaProducer<String,String>(producerProperties);
		
	}

	@Override
	public void write(StringRecord record) throws RecordWritingException {
		ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(topic, record.getPayload(), record.getPayload());
		producer.send(producerRecord);
	}

	@Override
	public void writeBatch(Collection<StringRecord> records) throws RecordWritingException {
		for(StringRecord rec:records){
			ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(topic, rec.getPayload(), rec.getPayload());
			producer.send(producerRecord);
		}
	}

	@Override
	public void close() throws IOException {
		producer.close();
		producer = null;
	}

}
