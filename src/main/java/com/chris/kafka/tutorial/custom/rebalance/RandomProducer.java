package com.chris.kafka.tutorial.custom.rebalance;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class RandomProducer {
	public static void main(String[] args) throws ParseException {
		System.out.println("Producer Runnig...");
		RandomProducer rp = new RandomProducer();
		rp.run();
		System.out.println("Done!");
	}

	private void run() throws ParseException {
		String topicName = "random-topic";

		Properties p = new Properties();
		p.put("bootstrap.servers", "localhost:9092,localhost:9093"); // brokers here
		p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Random rg = new Random();
		Calendar c = Calendar.getInstance();
		c.set(2016, 2, 2);
		String msg;
		try (Producer<String, String> prod = new KafkaProducer<>(p)) {
			while (true) {
				for (int i = 0; i < 100; i++) {
					msg = dateStr(c) + "," + rg.nextInt(1000);
					prod.send(new ProducerRecord<String, String>(topicName, 0, "K", msg));
					msg = dateStr(c) + "," + rg.nextInt(1000);
					prod.send(new ProducerRecord<String, String>(topicName, 1, "K", msg));
				}
				c.add(Calendar.DATE, 1);
				System.out.println("Data sent for: " + dateStr(c));
			}
		}
	}

	private String dateStr(Calendar c) {
		return c.get(Calendar.YEAR) + "-" + c.get(Calendar.MONTH) + "-" + c.get(Calendar.DATE);
	}

}
