package com.chris.kafka.tutorial.custom.serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

public class SupplierSerializer implements Serializer<Supplier> {

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
	}

	@Override
	public byte[] serialize(String topic, Supplier data) {
		int sizeOfName;
		int sizeOfDate;
		byte[] serializedName;
		byte[] serializedDate;

		if (data == null) {
			return null;
		}

		serializedName = data.getName().getBytes(StandardCharsets.UTF_8);
		sizeOfName = serializedName.length;

		serializedDate = data.getStartDate().toString().getBytes(StandardCharsets.UTF_8);
		sizeOfDate = serializedDate.length;

		ByteBuffer buf = ByteBuffer.allocate(4 + 4 + sizeOfName + 4 + sizeOfDate);

		buf.putInt(data.getId());

		buf.putInt(sizeOfName);
		buf.put(serializedName);

		buf.putInt(sizeOfDate);
		buf.put(serializedDate);

		return buf.array();
	}

	@Override
	public void close() {
	}

}
