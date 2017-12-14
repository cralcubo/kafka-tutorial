package com.chris.kafka.tutorial.custom.serializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SupplierConsumer {

	public static void main(String[] args) {
		System.out.println("Consumer Running...");
		SupplierConsumer sc = new SupplierConsumer();
		sc.run();
	}

	private void run() {
		String topicName = "supplier-topic";
		String groupID = "SupplierTopicGroup";

		Properties p = new Properties();
		p.put("bootstrap.servers", "localhost:9092,localhost:9093");
		p.put("group.id", groupID);
		p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		p.put("value.deserializer", "com.chris.kafka.tutorial.custom.serializer.SupplierDeserializer");

		try (KafkaConsumer<String, Supplier> consumer = new KafkaConsumer<>(p)) {
			consumer.subscribe(Arrays.asList(topicName));

			while (true) {
				Consumer<ConsumerRecord<String, Supplier>> c = r -> System.out.println("Supplier id=" + String.valueOf(r.value().getId()// 
																					+ " Supplier name=" + String.valueOf(r.value().getName()// 
																					+ " Supplier date=" + String.valueOf(r.value().getStartDate()))));//
				consumer.poll(100).forEach(c);
			}
		}

	}

}
