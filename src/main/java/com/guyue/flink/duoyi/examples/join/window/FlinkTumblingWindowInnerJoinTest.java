package com.guyue.flink.duoyi.examples.join.window;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName FlinkTumblingWindowInnerJoinTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-10 18:40
 */
public class FlinkTumblingWindowInnerJoinTest {

	public static void main(String[] args) throws Exception {

		// create env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// for testing;
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		// global configuration file
		String configPath = "/Users/lipeng/workspace_link/streamlink/streamlink-engine/streamlink-flink-learning/src/main/data/config.properties";
		ParameterTool parameterTool = ParameterTool.fromPropertiesFile(configPath);
		env.getConfig().setGlobalJobParameters(parameterTool);

		// parameters
		long windowSize = parameterTool.getLong("tumbling.window.size");
		long delayTime = parameterTool.getLong("window.delay.time");

		// checkpoint - path
		env.setStateBackend(new FsStateBackend(parameterTool.get("local.checkpoint.path")));

		// checkpoint configuration
		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.setCheckpointingMode(CheckpointingMode.valueOf(parameterTool.getRequired("checkpoint.mode")));
		checkpointConfig.setCheckpointInterval(parameterTool.getInt("checkpoint.interval"));
		checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.valueOf(parameterTool.getRequired("checkpoint.externalized")));

		// add left Source
		SingleOutputStreamOperator<Tuple3<String, String, Long>> leftStreamSource = env.addSource(new LeftDataSource()).name("left-Source-Stream");
		SingleOutputStreamOperator<Tuple3<String, String, Long>> leftStreamOperator = leftStreamSource
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delayTime)) {

				@Override
				public long getMaxOutOfOrdernessInMillis() {
					long maxOutOfOrdernessInMillis = super.getMaxOutOfOrdernessInMillis();
					System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", getMaxOutOfOrdernessInMillis= " + maxOutOfOrdernessInMillis);
					return maxOutOfOrdernessInMillis;
				}

				@Override
				public long extractTimestamp(Tuple3<String, String, Long> element) {
					System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", " + element);
					return element.f2;
				}
			});

		// add right Source

		SingleOutputStreamOperator<Tuple3<String, String, Long>> rightStreamSource = env.addSource(new RightDataSource()).name("right-Source-Stream");
		SingleOutputStreamOperator<Tuple3<String, String, Long>> rightStreamOperator = rightStreamSource
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delayTime)) {

				@Override
				public long getMaxOutOfOrdernessInMillis() {
					long maxOutOfOrdernessInMillis = super.getMaxOutOfOrdernessInMillis();
					System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", getMaxOutOfOrdernessInMillis= " + maxOutOfOrdernessInMillis);
					return maxOutOfOrdernessInMillis;
				}

				@Override
				public long extractTimestamp(Tuple3<String, String, Long> element) {
					System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", " + element);
					return element.f2;
				}
			});

		// join
		DataStream<Tuple5<String, String, String, Long, Long>> joinedStream = leftStreamOperator.join(rightStreamOperator)
			.where(new LeftKeySelector())
			.equalTo(new RightKeySelector())
			.window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
			.apply(new JoinInnerJoinFunction());

		joinedStream.print("join--------");

		env.execute("FlinkTumblingWindowInnerJoinTest");
	}

	public static class LeftKeySelector implements KeySelector<Tuple3<String, String, Long>, String> {

		@Override
		public String getKey(Tuple3<String, String, Long> left) throws Exception {
			return left.f0;
		}
	}

	public static class RightKeySelector implements KeySelector<Tuple3<String, String, Long>, String> {

		@Override
		public String getKey(Tuple3<String, String, Long> right) throws Exception {
			return right.f0;
		}
	}

	public static class JoinInnerJoinFunction implements
		JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple5<String, String, String, Long, Long>> {

		@Override
		public Tuple5<String, String, String, Long, Long> join(Tuple3<String, String, Long> first, Tuple3<String, String, Long> second)
			throws Exception {
			return Tuple5.of(first.f0, first.f1, second.f1, first.f2, second.f2);
		}
	}
}
