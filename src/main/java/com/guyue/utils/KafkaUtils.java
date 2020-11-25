package com.guyue.utils;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;

/**
 * @ClassName KafkaUtils
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-01-07 16:22
 */
public class KafkaUtils {


	public static Properties buildKafkaProperties(String groupId) {
		return buildKafkaProperties("localhost:9092", groupId);
	}

	public static Properties buildKafkaProperties(String servers, String groupId) {
		return buildKafkaProperties(servers, groupId, "earliest");
	}

	public static Properties buildKafkaProperties(String servers, String groupId, String offset) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", servers);
		properties.setProperty("auto.offset.reset", offset);
		properties.setProperty("group.id", groupId);
		properties.setProperty("enable.auto.commit", "false");
		return properties;
	}


}
