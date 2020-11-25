package com.guyue.flink.duoyi.examples.join.window;

import com.guyue.flink.duoyi.examples.join.window.FlinkTumblingWindowInnerJoinTest.LeftKeySelector;
import com.guyue.flink.duoyi.examples.join.window.FlinkTumblingWindowInnerJoinTest.RightKeySelector;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ClassName FlinkTumblingWindowLeftJoinTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-10 22:06
 */
public class FlinkTumblingWindowLeftJoinTest {

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
					System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", getMaxOutOfOrdernessInMillis= "
						+ maxOutOfOrdernessInMillis);
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
					System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", getMaxOutOfOrdernessInMillis= "
						+ maxOutOfOrdernessInMillis);
					return maxOutOfOrdernessInMillis;
				}

				@Override
				public long extractTimestamp(Tuple3<String, String, Long> element) {
					System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", " + element);
					return element.f2;
				}
			});

		// join
		DataStream<Tuple5<String, String, String, Long, Long>> joinedStream = leftStreamOperator.coGroup(rightStreamOperator)
			.where(new LeftKeySelector())
			.equalTo(new RightKeySelector())
			.window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
			.apply(new CoGroupedLeftJoinFunction());

		joinedStream.print("leftjoin--------");

		env.execute("FlinkTumblingWindowLeftJoinTest");
	}

	public static class CoGroupedLeftJoinFunction implements
		CoGroupFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple5<String, String, String, Long, Long>> {

		// coGroup 获取同一个计算窗口 left-Stream的数据和 right-Stream 的数据, 匹配相同的 Key 值.
		// 能够coGroup的数据满足以下几个条件:
		// 1. 两个Stream 的数据, 一定在同一个窗口中.
		// 2. 窗口已经被触发了.
		@Override
		public void coGroup(Iterable<Tuple3<String, String, Long>> firstIterator, Iterable<Tuple3<String, String, Long>> secondIterator,
							Collector<Tuple5<String, String, String, Long, Long>> collector) throws Exception {
			for (Tuple3<String, String, Long> first : firstIterator) {
				boolean isJoined = false;

				for (Tuple3<String, String, Long> second : secondIterator) {
					if (first.f0.equals(second.f0)) {

						collector.collect(Tuple5.of(first.f0, first.f1, second.f1, first.f2, second.f2));
						isJoined = true;

						// TODO do not break;
						// join 上之后, 继续与 右流 join, 因为有可能 匹配右流多条.
					}
				}

				// 左流没有 join 上右流
				if (!isJoined) {
					collector.collect(Tuple5.of(first.f0, first.f1, "null", first.f2, -1L));
				}
			}
		}
	}
}
