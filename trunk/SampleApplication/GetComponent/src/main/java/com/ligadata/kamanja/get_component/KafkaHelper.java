package com.ligadata.kamanja.get_component;

public class KafkaHelper {
	public void CheckVersion(){
//		System.out.println(kafka.api.OffsetRequest.CurrentVersion());
//		System.out.println(kafka.api.TopicMetadataRequest.CurrentVersion());

	}
	public void CheckKafkaVersion(String host){
		KafkaProducer pro = new KafkaProducer();
		pro.initialize(host);
		pro.CreateMessage();;
	}
}
