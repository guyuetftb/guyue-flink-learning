package com.guyue.flink.duoyi.examples.dyutil;

import com.guyue.utils.KafkaUtils;
import java.util.Properties;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @ClassName DYFlinkUtilV1
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-06 18:01
 */
public class DYFlinkUtilV1 {


	private static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


	public static DataStreamSource<String> createDataStreamSource(String[] args, SimpleStringSchema simpleStringSchema) {

		String kafkaServers = args[0];
		String topic = args[1];
		String groupId = args[2];

		// Kafka Source
		Properties kafkaProperties = KafkaUtils.buildKafkaProperties(kafkaServers, groupId);
		FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, simpleStringSchema, kafkaProperties);
		return env.addSource(stringFlinkKafkaConsumer);
	}

	public static StreamExecutionEnvironment getEnv() {
		return env;
	}
}
