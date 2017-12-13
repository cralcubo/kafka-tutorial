package com.chris.kafka.tutorial.custom.serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public class SupplierDeserializer implements Deserializer<Supplier> {

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {

	}

	@Override
	public Supplier deserialize(String topic, byte[] data) {
		if (data == null) {
			System.out.println("No data to deserialize!");
			return null;
		}

		ByteBuffer bb = ByteBuffer.wrap(data);

		int id = bb.getInt();

		String deserializedName = bufferBytesToString(bb);

		String deserializedDateStr = bufferBytesToString(bb);
		DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
		Date startDate = null;
		try {
			startDate = df.parse(deserializedDateStr);
		} catch (ParseException e1) {
			System.out.println("Error parsing the date[" + deserializedDateStr + "]");
		}

		return new Supplier(id, deserializedName, startDate);
	}

	@Override
	public void close() {

	}

	private String bufferBytesToString(ByteBuffer bb) {
		int size = bb.getInt();
		byte[] bytes = new byte[size];
		bb.get(bytes);

		return new String(bytes, StandardCharsets.UTF_8);
	}

}
