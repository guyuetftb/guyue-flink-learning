package com.guyue.flink.duoyi.examples.taskresearch;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName TaskSubTaskResearchTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-04 11:12
 */
public class TaskSubTaskResearchTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> localhostSocket = env.socketTextStream("localhost", 8888);
		System.out.println(" Socket Stream parallel = " + localhostSocket.getParallelism());

		// flat map
		SingleOutputStreamOperator<Tuple2<String, Integer>> ricFlatMap = localhostSocket
			.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {

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
						collector.collect(Tuple2.of(word.toLowerCase(), 1));
					}
				}

				@Override
				public void close() throws Exception {
					System.out.println("RichFlatMapFunction +++++++> close()" + Thread.currentThread().getName());
				}
			});

		System.out.println(" Rich Flat-map parallel = " + ricFlatMap.getParallelism());


		// 演示 disableChaining
		SingleOutputStreamOperator<Tuple2<String, Integer>> filterNewChain = ricFlatMap
			.filter(new RichFilterFunction<Tuple2<String, Integer>>() {
				@Override
				public boolean filter(Tuple2<String, Integer> value) throws Exception {
					return value.f0.length() > 2;
				}
			}).disableChaining().name(" I am DisableChaining");

		// 过滤后
		SingleOutputStreamOperator<Tuple2<String, Integer>> mapUpperCase = filterNewChain
			.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

				@Override
				public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
					return Tuple2.of(value.f0.toUpperCase(), value.f1);
				}
			});



		// keyBy
		SingleOutputStreamOperator<Tuple2<String, Integer>> sumStream = mapUpperCase.keyBy(0).sum(1);
		System.out.println(" Keyed Stream parallel = " + sumStream.getParallelism());

		sumStream.print().setParallelism(2);

		env.execute("TaskSubTaskResearchTest");
	}
}
