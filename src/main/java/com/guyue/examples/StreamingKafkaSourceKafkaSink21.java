package com.guyue.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Created by lipeng com.guyue.flink lipeng 2019/4/18
 */
public class StreamingKafkaSourceKafkaSink21 {

	private static String sourceTopic = "flinkMysqlJoin";
	private static String sinkTopic = "flinkMysqlJoinMul";

	public static void main(String[] args) {

		Properties sourceKafkaProperties = new Properties();
		sourceKafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
		sourceKafkaProperties.setProperty("group.id", "gy_group_source");

		// ENV, note: there has 2 StreamExecutionEnvironment.
		final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

		// SOURCE
		FlinkKafkaConsumer<String> myKafka010Consumer = new FlinkKafkaConsumer<String>(sourceTopic, new SimpleStringSchema(StandardCharsets.UTF_8), sourceKafkaProperties);
		myKafka010Consumer.setStartFromEarliest();  // 从最早的记录消费

		DataStreamSource dss = see.addSource(myKafka010Consumer);
		dss.map(new MapFunction<String, String>() {

			@Override
			public String map(String value) throws Exception {
				System.out.println(" line -> " + value);
				return value;
			}
		});

		// SINK
		Properties sinkKafkaProperties = new Properties();
		sinkKafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
		sinkKafkaProperties.setProperty("group.id", "gy_group_sink");
		FlinkKafkaProducer<String> myKafka010Producer = new FlinkKafkaProducer<String>(sinkTopic, new SimpleStringSchema(), sinkKafkaProperties);
		dss.addSink(myKafka010Producer);

		// EXE
		// gy_kafka_sink_kafka_source
		try {
			see.execute("gy_kafka_sink_kafka_source");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
