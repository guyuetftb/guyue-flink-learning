package com.guyue.flink.duoyi.examples.checkpoint;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName RestartingStrategies
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-07 14:56
 */
public class RestartingStrategies {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 开启了 Checkpoint, 才会有重启策略
		env.enableCheckpointing(5000);
		ExecutionConfig executionConfig = env.getConfig();
		// 设置重启策略.
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.seconds(1)));

		DataStreamSource<String> localhostSocket = env.socketTextStream("localhost", 8888);

		SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = localhostSocket.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(String line) throws Exception {
				if (line.startsWith("shutdown")) {
					throw new RuntimeException(" I am shutdown !");
				}
				return Tuple2.of(line.trim().toUpperCase(), 1);
			}
		});

		SingleOutputStreamOperator<Tuple2<String, Integer>> sumKeyedStream = mapStream.keyBy(0).sum(1);
		sumKeyedStream.print();
		env.execute("RestartingStrategies");
	}
}
