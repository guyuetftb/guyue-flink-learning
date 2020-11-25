package com.guyue.flink.duoyi.examples.transformation;

import com.guyue.utils.KafkaUtils;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
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
public class KafkaSourceAggregateTest {

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
		SingleOutputStreamOperator<Tuple2<String, Integer>> mapWordFreq = lines.map(new RichMapFunction<String, Tuple2<String, Integer>>() {

			@Override
			public void open(Configuration parameters) throws Exception {
				System.out.println("RichMapFunction +++++++> open()" + Thread.currentThread().getName());
			}

			@Override
			public Tuple2<String, Integer> map(String s) throws Exception {
				if(StringUtils.isEmpty(s)){
					return Tuple2.of("", 0);
				}
				String[] wordsFreq = s.trim().split(",");
				return Tuple2.of(wordsFreq[0], Integer.valueOf(wordsFreq[1]));
			}

			@Override
			public void close() throws Exception {
				System.out.println("RichMapFunction +++++++> close()" + Thread.currentThread().getName());
			}
		});


		// 在 Java 中的元组的下标从0开始, 而且 Java 的元组最多支持到25个元素.
		// 在 Scala 中元组的下标从1开始.

		// keyBy 支持多字段分组, 类似于 Sql 中的 groupBy.
		// 根据 第1个元素 word 对元素进行分组, word 相同的Tuple2都会发送到同一个 SubTask 中。
		KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = mapWordFreq.keyBy(0);

		// Sink print
		// Flink 在内部维护一个状态, 记录下分组后, 指定 Key 的最大值.
		SingleOutputStreamOperator<Tuple2<String, Integer>> max = tuple2TupleKeyedStream.max(1);
		max.print();

		env.execute("KafkaSourceAggregateTest");
	}
}
