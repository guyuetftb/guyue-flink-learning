package com.guyue.flink.duoyi.examples.transformation;

import com.guyue.utils.KafkaUtils;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * @ClassName KafkaSourceSimpleTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-03 12:11
 */
public class KafkaSourceFlatMapTest {

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
		SingleOutputStreamOperator<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {

			public void flatMap(String s, Collector<String> collector) throws Exception {
				String[] words = s.split("\\s+");
				for (String word : words) {
					if (StringUtils.isEmpty(word)) {
						continue;
					}
					collector.collect("FlatMap - " + word.toLowerCase());
				}
			}
		});

		// method 2
		// lines.map(i -> Integer.valueOf(i) * 2).returns(Integer.class);

		// method 3, rich-map
		SingleOutputStreamOperator<String> ricFlatMap = lines.flatMap(new RichFlatMapFunction<String, String>() {


			@Override
			public void open(Configuration parameters) throws Exception {
				System.out.println("RichFlatMapFunction +++++++> open()" + Thread.currentThread().getName());
			}

			@Override
			public void flatMap(String s, Collector collector) throws Exception {
				String[] words = s.split("\\s+");
				for (String word : words) {
					if (StringUtils.isEmpty(word)) {
						continue;
					}
					collector.collect("RichFlatMapFunction - " + word.toLowerCase());
				}
			}

			@Override
			public void close() throws Exception {
				System.out.println("RichFlatMapFunction +++++++> close()" + Thread.currentThread().getName());
			}
		});

		// Sink print
		flatMap.print();
		// Sink print
		ricFlatMap.print();

		env.execute("KafkaSourceFlatMapTest");
	}

}
