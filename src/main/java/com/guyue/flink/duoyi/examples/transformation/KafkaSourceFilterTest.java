package com.guyue.flink.duoyi.examples.transformation;

import com.guyue.utils.KafkaUtils;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
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
public class KafkaSourceFilterTest {

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
		SingleOutputStreamOperator<String> filter = lines.filter(new FilterFunction<String>() {

			@Override
			public boolean filter(String s) throws Exception {
				if (s.length() > 2) {
					return true;
				}
				return false;
			}
		});

		// lines.map(i -> Integer.valueOf(i) * 2).returns(Integer.class);

		SingleOutputStreamOperator<String> richFilter = lines.filter(new RichFilterFunction<String>() {

			@Override
			public void open(Configuration parameters) throws Exception {
				System.out.println("RichFilterFunction +++++++> open()" + Thread.currentThread().getName());
			}

			@Override
			public boolean filter(String s) throws Exception {
				if (s.length() > 2) {
					return true;
				}
				return false;
			}

			@Override
			public void close() throws Exception {
				System.out.println("RichFilterFunction +++++++> close()" + Thread.currentThread().getName());
			}
		});

		// Sink print
		filter.print();
		// Sink print
		richFilter.print();

		env.execute("KafkaSourceFilterTest");
	}

}
