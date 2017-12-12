package com.chris.kafka.tutorial.partition.custom;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MultiPartitionProducer {

	public static void main(String[] args) {
		System.out.println("Starting...");
		MultiPartitionProducer mpp = new MultiPartitionProducer();
		mpp.run();
		System.out.println("End!");
	}

	public void run() {
		String topicName = "sensor-topic";

		Properties p = new Properties();
		p.put("bootstrap.servers", "localhost:9092,localhost:9093");
		p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		p.put("partitioner.class", "com.chris.kafka.tutorial.partition.custom.CustomizedPartitioner");
		p.put("speed.sensor.name", "TSS");

		Producer<String, String> producer = new KafkaProducer<>(p);

		for (int i = 0; i < 10; i++) {
			producer.send(new ProducerRecord<String, String>(topicName, "SSP" + i, "500" + i));
		}
		
		for (int i = 0; i < 10; i++) {
			producer.send(new ProducerRecord<String, String>(topicName, "TSS", "500" + i));
		}

		producer.close();
	}

}
