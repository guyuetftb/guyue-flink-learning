package com.guyue.flink.duoyi.examples.transformation;

import com.guyue.utils.KafkaUtils;
import java.util.Properties;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @ClassName KafkaSourceSimpleTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-03 12:11
 */
public class KafkaSourceMapTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Kafka Source
		String topics = "wc10";
		Properties kafkaProperties = KafkaUtils.buildKafkaProperties("gid-dy-wc01");
		SimpleStringSchema stringSchema = new SimpleStringSchema();
		FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>(topics, stringSchema, kafkaProperties);

		// Source add
		DataStreamSource<String> lines = env.addSource(stringFlinkKafkaConsumer);

		// method 1
		SingleOutputStreamOperator<Integer> map = lines.map(new MapFunction<String, Integer>() {

			@Override
			public Integer map(String s) throws Exception {
				return Integer.valueOf(s) * 2;
			}
		});

		// method 2
		// lines.map(i -> Integer.valueOf(i) * 2).returns(Integer.class);

		// method 3, rich-map
		SingleOutputStreamOperator<Integer> richMap = lines.map(new RichMapFunction<String, Integer>() {


			@Override
			public void open(Configuration parameters) throws Exception {
				System.out.println("RichMapFunction +++++++> open()");
			}

			@Override
			public Integer map(String s) throws Exception {
				return Integer.valueOf(s) * 2;
			}

			@Override
			public void close() throws Exception {
				System.out.println("RichMapFunction +++++++> close()");
			}
		});

		// Sink print
		map.print();
		// Sink print
		richMap.print();

		env.execute("KafkaSourceMapTest");
	}

}
