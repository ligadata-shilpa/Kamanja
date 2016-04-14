package com.ligadata.twitterfeeds.kafkaconsumer;

import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerExample {

    String topicName = "koko";
    String topicName2 = "koko";

    KafkaConsumer<String, String> kafkaConsumer = null;
    KafkaConsumer<String, String> kafkaConsumer2 = null;

    public void kickOff(String args[]) {
        if (args.length == 0) {
            System.out.println("ERROR: Pass properties file as parameter");
            System.exit(1);
        }

        String inputPropertiesFile = args[0];
        String inputPropertiesFile2 = args[1];
        Properties props = new Properties();
        Properties props2 = new Properties();
        try {
            System.out
                    .println("Loading properties file " + inputPropertiesFile);
            props.load(new FileReader(new File(inputPropertiesFile)));
            props2.load(new FileReader(new File(inputPropertiesFile2)));
            setup(props,props2);
            startConsumer();
            kafkaConsumer.close();
            kafkaConsumer2.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void setup(Properties props,Properties props2) {
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");

        props2.put("key.deserializer", StringDeserializer.class.getName());
        props2.put("value.deserializer", StringDeserializer.class.getName());

        props2.put("enable.auto.commit", "true");
        props2.put("auto.commit.interval.ms", "1000");
        props2.put("session.timeout.ms", "30000");

        props2.put("security.protocol", "SASL_PLAINTEXT");
        props2.put("sasl.kerberos.service.name", "kafka");

        if (props.get("topic_name") != null) {
            topicName = (String) props.get("topic_name");
            topicName2 = (String) props2.get("topic_name");
            System.out.println("Consuming from topic=" + topicName);
        }
        kafkaConsumer = new KafkaConsumer<String, String>(props);
        kafkaConsumer2 = new KafkaConsumer<String, String>(props2);
        kafkaConsumer.subscribe(Arrays.asList(topicName));
        kafkaConsumer2.subscribe(Arrays.asList(topicName2));
    }

    public void startConsumer() {
        System.out.println("Listening on topic=" + topicName
                + ", kafkaConsumer=" + kafkaConsumer);
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            ConsumerRecords<String, String> records2 = kafkaConsumer2.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();

                System.out.println((key != null ? "key=" + key + ", " : "")
                        + "value=" + value);
            }

            for (ConsumerRecord<String, String> record : records2) {
              String key = record.key();
              String value = record.value();

              System.out.println((key != null ? "key=" + key + ", " : "")
                      + "value=" + value);
          }

        }

    }

    static public void main(String args[]) {
        KafkaConsumerExample consumer = new KafkaConsumerExample();
        consumer.kickOff(args);
    }

}