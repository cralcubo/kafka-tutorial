package com.chris.kafka.tutorial.custom.rebalance;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class RandomConsumer {
	
	public static void main(String[] args) {
		System.out.println("Consumer Running...");
		RandomConsumer rc = new RandomConsumer();
		rc.run();
	}

	private void run() {
		String topicName = "random-topic";
		String groupID = "RandomTopicGroup";

		Properties p = new Properties();
		p.put("bootstrap.servers", "localhost:9092,localhost:9093");
		p.put("group.id", groupID);
		p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		p.put("enable.auto.comit", "false");
		
		
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
			RebalanceListener rl = new RebalanceListener(consumer);
			consumer.subscribe(Arrays.asList(topicName),rl);

			while (true) {
				// Do some processing...
				consumer.poll(100).forEach(cr -> rl.addOffset(cr.topic(), cr.partition(), cr.offset()));
			}
		}

	}

}
