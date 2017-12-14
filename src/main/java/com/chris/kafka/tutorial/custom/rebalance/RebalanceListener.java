package com.chris.kafka.tutorial.custom.rebalance;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class RebalanceListener implements ConsumerRebalanceListener {
	private KafkaConsumer<String, String> consumer;
	private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

	public RebalanceListener(KafkaConsumer<String, String> consumer) {
		this.consumer = consumer;
	}

	public void addOffset(String topic, int partition, long offset) {
		currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset, ""));
	}

	public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
		return currentOffsets;
	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		System.out.println("Following partition assigned");
		partitions.forEach(p -> System.out.println(p.partition()));
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		System.out.println("Following partition revoked");
		partitions.forEach(p -> System.out.println(p.partition()));

		System.out.println("Following partition commited");
		currentOffsets.keySet().forEach(tp -> System.out.println(tp.partition()));

		consumer.commitSync(currentOffsets);
		currentOffsets.clear();
	}
}
