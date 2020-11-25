package com.guyue.flink.duoyi.examples.broadcast;

import com.guyue.flink.duoyi.examples.dimension.bean.ActivityBean;
import com.guyue.flink.duoyi.examples.dyutil.FlinkUtil;
import java.io.File;
import java.lang.reflect.Parameter;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.util.Collector;

/**
 * @ClassName BroadcastTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-11 23:01
 */
public class BroadcastTest {

	public static void main(String[] args) throws Exception {

		String configPath = "/Users/lipeng/workspace_link/streamlink/streamlink-engine/streamlink-flink-learning/src/main/data/config.properties";
		ParameterTool parameterTool = ParameterTool.fromPropertiesFile(new File(configPath));

		StreamExecutionEnvironment env = FlinkUtil.getEnv();
		ExecutionConfig envConfig = env.getConfig();
		envConfig.setGlobalJobParameters(parameterTool);
		envConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(2000, 1000));

		// parameters
		String streamTimeCharacteristic = parameterTool.get("stream.time.characteristic");
		String localCheckpointPath = parameterTool.get("local.checkpoint.path");
		String checkpointMode = parameterTool.get("checkpoint.mode");
		long checkpointInterval = parameterTool.getInt("checkpoint.interval");
		String checkpointExternalized = parameterTool.get("checkpoint.externalized");

		// stream configuration.
		env.setStateBackend(new FsStateBackend(localCheckpointPath));
		env.setStreamTimeCharacteristic(TimeCharacteristic.valueOf(streamTimeCharacteristic));
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2000, 1000));

		// checkpint
		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.setCheckpointInterval(checkpointInterval);
		checkpointConfig.setCheckpointingMode(CheckpointingMode.valueOf(checkpointMode));
		checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.valueOf(checkpointExternalized));

		String topic = parameterTool.get("kafka.topic");
		String gid = parameterTool.get("kafka.group.id");

		SingleOutputStreamOperator<Tuple3<String, String, String>> dictStream = FlinkUtil
			.createKafkaDataStream(parameterTool, topic, gid, SimpleStringSchema.class)
			.process(new ProcessFunction<String, Tuple3<String, String, String>>() {
				@Override
				public void processElement(String activity, Context ctx, Collector<Tuple3<String, String, String>> collector) throws Exception {
					String[] activitiesArr = activity.split(",");
					collector.collect(Tuple3.of(activitiesArr[0], activitiesArr[1], activitiesArr[2]));
				}
			});

		final MapStateDescriptor dictMapStateDescriptor = new MapStateDescriptor("dict-broadstate",
			TypeInformation.of(new TypeHint<String>() {
			}),
			TypeInformation.of(new TypeHint<String>() {
			})
		);
		BroadcastStream<Tuple3<String, String, String>> dictBroadcastStream = dictStream.broadcast(dictMapStateDescriptor);

		DataStreamSource<String> localhostSocketStream = env.socketTextStream("localhost", 8888);
		SingleOutputStreamOperator<Tuple3<String, String, String>> activityDetailStream = localhostSocketStream
			.map(new RichMapFunction<String, Tuple3<String, String, String>>() {

				@Override
				public Tuple3<String, String, String> map(String activityDetail) throws Exception {
					String[] activityArr = activityDetail.split(",");
					return Tuple3.of(activityArr[0], activityArr[1], activityArr[1]);
				}
			});

		SingleOutputStreamOperator<Tuple4<String, String, String, String>> connectedStream = activityDetailStream.connect(dictBroadcastStream)
			.process(new BroadcastProcessFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple4<String, String, String, String>>() {
				@Override
				public void processElement(Tuple3<String, String, String> activity, ReadOnlyContext ctx,
										   Collector<Tuple4<String, String, String, String>> collector) throws Exception {

					ReadOnlyBroadcastState<String, String> broadcastStateReadOnly = ctx.getBroadcastState(dictMapStateDescriptor);
					String name = broadcastStateReadOnly.get(activity.f1);

					collector.collect(Tuple4.of(activity.f0, activity.f1, name, activity.f2));
				}

				@Override
				public void processBroadcastElement(Tuple3<String, String, String> broadValue, Context ctx,
													Collector<Tuple4<String, String, String, String>> out) throws Exception {
					System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", " + broadValue);

					BroadcastState broadcastState = ctx.getBroadcastState(dictMapStateDescriptor);
					if (broadValue.f2.equalsIgnoreCase("DELETE")) {
						broadcastState.remove(broadValue.f0);
					} else {
						broadcastState.put(broadValue.f0, broadValue.f1);
					}

				}
			});

		connectedStream.print("connect------------>");

		env.execute("BroadcastTest");

	}
}
