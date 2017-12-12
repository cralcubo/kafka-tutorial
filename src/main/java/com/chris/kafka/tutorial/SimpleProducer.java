package com.chris.kafka.tutorial;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class SimpleProducer {
	
	public static void main(String[] args) {
		SimpleProducer sp = new SimpleProducer();
		sp.run();
		System.out.println("End!");
	}
	
	public void run() {
		String topicName = "test-topic";
		
		Properties p = new Properties();
		p.put("bootstrap.servers", "localhost:9092");
		p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String, String> producer = new KafkaProducer<>(p);
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, "KEY", "VALUE");
		producer.send(record);
		producer.close();
	}
}
