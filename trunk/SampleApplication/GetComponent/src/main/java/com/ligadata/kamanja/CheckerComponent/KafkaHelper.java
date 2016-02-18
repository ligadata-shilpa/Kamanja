package com.ligadata.kamanja.CheckerComponent;

import java.util.ArrayList;
import java.util.List;

import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaHelper {
	KafkaProducer pro = new KafkaProducer();
	public void CheckVersion(){
//		System.out.println(kafka.api.OffsetRequest.CurrentVersion());
//		System.out.println(kafka.api.TopicMetadataRequest.CurrentVersion());
	}
	public void GetTopics(){
		kafka.javaapi.consumer.SimpleConsumer consumer = new SimpleConsumer("localhost",9092,100000,64*1024,"test");
		List<String> topic2 = new ArrayList<String>();
		TopicMetadataRequest req = new TopicMetadataRequest(topic2);
		kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
		List<kafka.javaapi.TopicMetadata> data3 = resp.topicsMetadata();
		for (kafka.javaapi.TopicMetadata item : data3) {
            System.out.println("Topic: " + item.topic());
            for (kafka.javaapi.PartitionMetadata part: item.partitionsMetadata() ) {
                String replicas = "";
                String isr = "";
                for (kafka.cluster.Broker replica: part.replicas() ) {
                    replicas += " " + replica.host();
                }
                for (kafka.cluster.Broker replica: part.isr() ) {
                    isr += " " + replica.host();
                }
                System.out.println( "    Partition: " +   part.partitionId()  + ": Leader: " + part.leader().host() + " Replicas:[" + replicas + "] ISR:[" + isr + "]");
            }
        }
	}
	public void AskKafka(String host){
		
		//pro.initialize(host);
		GetTopics();
		//pro.CreateMessage();;
	}
}
