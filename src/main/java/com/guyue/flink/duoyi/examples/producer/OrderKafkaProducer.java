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
public class OrderKafkaProducer {

	public static void main(String[] args) {

		String orderMainTopic = "order_main";
		String orderDetailTopic = "order_detail";

		Properties producerPro = new Properties();
		producerPro.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		/* default configuration */
		producerPro.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		producerPro.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		producerPro.put(ProducerConfig.LINGER_MS_CONFIG, 100);
		producerPro.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
		producerPro.put(ProducerConfig.RETRIES_CONFIG, 100);

		KafkaProducer kafkaProducer = new KafkaProducer<String, String>(producerPro);

		long currentTime = System.currentTimeMillis();

		String orderMainJson = "{\"city\":\"北京\",\"count\":1,\"createTime\":" + currentTime
			+ ",\"orderId\":100001,\"province\":\"北京\",\"status\":1,\"totalMoney\":1008.5,\"type\":\"INSERT\",\"updateTime\":" + currentTime + "}";
		ProducerRecord orderMainRecord = new ProducerRecord(orderMainTopic, "100001", orderMainJson);
		kafkaProducer.send(orderMainRecord);

		String orderDetailJson1 = "{\"amount\":1,\"categoryName\":\"食品\",\"count\":1,\"createTime\":" + currentTime
			+ ",\"id\":200001,\"money\":8.0,\"orderId\":100001,\"sku\":\"sku-food-0001\",\"type\":\"INSERT\",\"updateTime\":" + currentTime + "}";
		ProducerRecord orderDetailRecord1 = new ProducerRecord(orderDetailTopic, "100001", orderDetailJson1);
		kafkaProducer.send(orderDetailRecord1);

		String orderDetailJson2 = "{\"amount\":1,\"categoryName\":\"工具\",\"count\":1,\"createTime\":" + currentTime
			+ ",\"id\":200002,\"money\":1000.0,\"orderId\":100001,\"sku\":\"sku-tool-0002\",\"type\":\"INSERT\",\"updateTime\":" + currentTime + "}";
		ProducerRecord orderDetailRecord2 = new ProducerRecord(orderDetailTopic, "100001", orderDetailJson2);
		kafkaProducer.send(orderDetailRecord2);
		kafkaProducer.close();
	}
}
