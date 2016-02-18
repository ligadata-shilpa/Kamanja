package com.ligadata.kamanja.CheckerComponent;

import java.util.ArrayList;
import java.util.List;

import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaHelper {
    // KafkaProducer pro = new KafkaProducer();

    String errorMessage = null;
    String status = null;

    public void CheckVersion() {
//		System.out.println(kafka.api.OffsetRequest.CurrentVersion());
//		System.out.println(kafka.api.TopicMetadataRequest.CurrentVersion());
    }

    public void GetTopics(String hostslist) {
        String[] hlists = hostslist.split(",");
        boolean gotHosts = false;
        int i = 0;
        Throwable error = null; // Getting one error is enough to send back.
        while (!gotHosts && i < hlists.length) {
            String broker = hlists[i];
            try {
                String[] brokerNameAndPort = broker.split(":");
                String brokerName = brokerNameAndPort[0].trim();
                String portStr = "9092";
                if (brokerNameAndPort.length > 1)
                    portStr = brokerNameAndPort[1].trim();
                int port = Integer.parseInt(portStr);

                SimpleConsumer consumer = new SimpleConsumer(brokerName, port, 100000, 64 * 1024, "test");
                List<String> topic2 = new ArrayList<String>();
                TopicMetadataRequest req = new TopicMetadataRequest(topic2);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
                List<kafka.javaapi.TopicMetadata> data3 = resp.topicsMetadata();
                for (kafka.javaapi.TopicMetadata item : data3) {
                    System.out.println("Topic: " + item.topic());
                    for (kafka.javaapi.PartitionMetadata part : item.partitionsMetadata()) {
                        String replicas = "";
                        String isr = "";
                        for (kafka.cluster.Broker replica : part.replicas()) {
                            replicas += " " + replica.host();
                        }
                        for (kafka.cluster.Broker replica : part.isr()) {
                            isr += " " + replica.host();
                        }
                        System.out.println("    Partition: " + part.partitionId() + ": Leader: " + part.leader().host() + " Replicas:[" + replicas + "] ISR:[" + isr + "]");
                    }
                }
                gotHosts = true;
            } catch (Exception e) {
                error = e;
            } catch (Throwable t) {
                error = t;
            }
            i += 1;
        }

        if (!gotHosts && error != null) {
            errorMessage = new StringUtility().getStackTrace(error);
            status = "Fail";
        } else {
            status = "Success";
        }
    }

    public void AskKafka(String hostslist) {
        //pro.initialize(host);
        GetTopics(hostslist);
        //pro.CreateMessage();;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getStatus() {
        return status;
    }
}
