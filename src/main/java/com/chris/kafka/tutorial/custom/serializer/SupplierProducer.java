package com.chris.kafka.tutorial.custom.serializer;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SupplierProducer {

	public static void main(String[] args) throws ParseException {
		System.out.println("Supplier Runnig...");
		SupplierProducer sp = new SupplierProducer();
		sp.run();
		System.out.println("Done!");
	}

	private void run() throws ParseException {
		String topicName = "supplier-topic";

		Properties p = new Properties();
		p.put("bootstrap.servers", "localhost:9092,localhost:9093"); //brokers here
		p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		p.put("value.serializer", "com.chris.kafka.tutorial.custom.serializer.SupplierSerializer");
		
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Supplier sp1 = new Supplier(555, "supplier-one", df.parse("2017-02-17"));
		Supplier sp2 = new Supplier(777, "supplier-two", df.parse("2018-12-25"));
		
		try (Producer<String, Supplier> prod = new KafkaProducer<>(p)) {
			prod.send(new ProducerRecord<String, Supplier>(topicName, sp1));
			prod.send(new ProducerRecord<String, Supplier>(topicName, sp2));
		}
	}

}
