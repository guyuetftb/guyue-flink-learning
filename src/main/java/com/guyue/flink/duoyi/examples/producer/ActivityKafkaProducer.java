package com.guyue.flink.duoyi.examples.producer;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @ClassName KafkaConsumer
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-11 18:06
 */
public class ActivityKafkaProducer {

	public static void main(String[] args) {

		String topic = "activity_topic";

		Properties producerPro = new Properties();
		producerPro.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		/* default configuration */
		producerPro.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		producerPro.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		producerPro.put(ProducerConfig.LINGER_MS_CONFIG, 100);
		producerPro.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
		producerPro.put(ProducerConfig.RETRIES_CONFIG, 100);

		KafkaProducer kafkaProducer = new KafkaProducer<String, String>(producerPro);
		String[] activities = new String[]{
			"A1,新人礼包,INSERT",
			"A2,促销活动,INSERT"
		};

		activities = new String[]{
			"A3,测试活动,INSERT",
			"A4,双11活动,INSERT"
		};

		activities = new String[]{
			"A3,测试活动,DELETE"
		};

		for (String a : activities) {
			ProducerRecord r = new ProducerRecord(topic, null, a);
			kafkaProducer.send(r);
		}
		kafkaProducer.close();
	}
}
