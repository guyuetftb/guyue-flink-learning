package com.guyue.flink.duoyi.examples.transformation;

import com.guyue.utils.KafkaUtils;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichFoldFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
public class KafkaSourceFoldTest {

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
		SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapWordFreq = lines
			.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {

				@Override
				public void open(Configuration parameters) throws Exception {
					System.out.println("RichFlatMapFunction +++++++> open()" + Thread.currentThread().getName());
				}

				@Override
				public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
					String[] words = s.split("\\s+");
					for (String word : words) {
						if (StringUtils.isEmpty(word)) {
							continue;
						}
						collector.collect(Tuple2.of(word.toLowerCase(), 1));
					}
				}

				@Override
				public void close() throws Exception {
					System.out.println("RichFlatMapFunction +++++++> close()" + Thread.currentThread().getName());
				}
			});

		// 在 Java 中的元组的下标从0开始, 而且 Java 的元组最多支持到25个元素.
		// 在 Scala 中元组的下标从1开始.

		// keyBy 支持多字段分组, 类似于 Sql 中的 groupBy.
		KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = flatMapWordFreq.keyBy(0);

		// Sink print
		SingleOutputStreamOperator<Tuple2<String, Integer>> fold = tuple2TupleKeyedStream
			.fold(new Tuple2<String, Integer>("", 0), new RichFoldFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

				@Override
				public Tuple2<String, Integer> fold(Tuple2<String, Integer> accumulator, Tuple2<String, Integer> o) throws Exception {
					String word = accumulator.f0;
					int sum = accumulator.f1;
					if (accumulator.f0.equals("")) {
						word = o.f0;
						sum += o.f1;
					} else {
						word = word + "-" + o.f0;
						sum += o.f1;
					}
					return Tuple2.of(word, sum);
				}
			});

		fold.print();

		env.execute("KafkaSourceKeyByTest");
	}
}
