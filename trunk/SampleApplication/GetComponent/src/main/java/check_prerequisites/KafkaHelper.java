package check_prerequisites;

public class KafkaHelper {

	public void CheckKafkaVersion(String host){
		KafkaProducer pro = new KafkaProducer();
		pro.initialize(host);
		pro.CreateMessage();;
	}
}
