package com.guyue.examples;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName TestKafkaConsumer
 * @Description TOOD
 * @Author lipeng
 * @Date 2019/7/3 11:28
 */
public class TestKafkaConsumer {

	private static Logger log = LoggerFactory.getLogger(TestKafkaConsumer.class);

	public static void main(String[] args) {
		String topic = "test";
		Properties consumerPro = new Properties();
		consumerPro.setProperty("max.poll.records", "1");
		consumerPro.setProperty("bootstrap.servers", "localhost:9092");
		consumerPro.setProperty("group.id", "rtalgschfturetl_udf_001");
		consumerPro.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumerPro.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerPro);
		consumer.subscribe(Collections.singletonList(topic));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(2000);
			System.out.println(System.currentTimeMillis() + ">>>>>>>>>>>>>>>>>>>>>>>>" + records.isEmpty());
			for (ConsumerRecord<String, String> record : records)  //3)
			{
				System.out.println(System.currentTimeMillis() + "------------------- topic=" + record.topic());
				System.out.println(System.currentTimeMillis() + "------------------- partition=" + record.partition());
				System.out.println(System.currentTimeMillis() + "------------------- offset=" + record.offset());
				System.out.println(System.currentTimeMillis() + "------------------- key=" + record.key());
				System.out.println(System.currentTimeMillis() + "------------------- value=" + record.value());
			}
		}
	}
}
