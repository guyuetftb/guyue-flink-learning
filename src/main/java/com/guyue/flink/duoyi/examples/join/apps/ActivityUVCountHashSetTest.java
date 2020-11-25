package com.guyue.flink.duoyi.examples.join.apps;

import com.guyue.flink.duoyi.examples.dimension.bean.ActivityBean;
import com.guyue.flink.duoyi.examples.dyutil.FlinkUtil;
import com.guyue.flink.duoyi.examples.join.apps.bean.OrderMain;
import java.io.File;
import java.util.HashSet;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName ActivityUVCountHashSetTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-11 19:00
 */
public class ActivityUVCountHashSetTest {

	public static void main(String[] args) throws Exception {
		// DataStreamSource<String> localhostSocket = env.socketTextStream("localhost", 8888);
		// 左表数据 Join 右表数据, 右表迟到没有 Join 上, 直接查询数据库.
		// 左表数据迟到, 使用测输出.
		StreamExecutionEnvironment env = FlinkUtil.getEnv();
		ExecutionConfig envConfig = env.getConfig();
		envConfig.setParallelism(1);

		// global configuration
		String configPath = "/Users/lipeng/workspace_link/streamlink/streamlink-engine/streamlink-flink-learning/src/main/data/config.properties";
		ParameterTool parameterTool = ParameterTool.fromPropertiesFile(new File(configPath));
		envConfig.setGlobalJobParameters(parameterTool);

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

		KeyedStream<ActivityBean, Tuple> activityKeyedStream = FlinkUtil
			.createKafkaDataStream(parameterTool, topic, gid, SimpleStringSchema.class)
			.map(new RichMapFunction<String, ActivityBean>() {
				@Override
				public ActivityBean map(String line) throws Exception {
					// u001,A1,2019-09-02 10:10:11,1,北京市
					String[] lineArr = line.split(",");

					String activityName = null;
					String uid = lineArr[0];
					String aid = lineArr[1];
					String time = lineArr[2];
					int eventType = Integer.parseInt(lineArr[3]);
					String province = lineArr[4];
					return ActivityBean.of(uid, aid, "null", time, eventType, province);
				}
			}).keyBy("aid", "eventType");

		SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> activityGroup = activityKeyedStream
			.map(new RichMapFunction<ActivityBean, Tuple3<String, Integer, Integer>>() {

				private transient ValueState uidState = null;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					ValueStateDescriptor uidDescriptor = new ValueStateDescriptor("uid-set", HashSet.class);
					uidState = getRuntimeContext().getState(uidDescriptor);
				}

				@Override
				public void close() throws Exception {
					super.close();
					uidState.clear();
				}

				@Override
				public Tuple3<String, Integer, Integer> map(ActivityBean activityBean) throws Exception {
					HashSet uidSet = (HashSet) uidState.value();
					if (null == uidSet) {
						uidSet = new HashSet();
					}
					uidSet.add(activityBean.uid);
					uidState.update(uidSet);

					return Tuple3.of(activityBean.aid, activityBean.eventType, uidSet.size());
				}
			});

		DataStreamSource<String> localhostSocket = env.socketTextStream("localhost", 8888);
		SingleOutputStreamOperator<String> map = localhostSocket.map(new RichMapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				if (value.startsWith("ERROR")) {
					System.out.println(1 / 0);
				}
				return value;
			}
		});

		activityGroup.print("uid-uv-count-hashset-");

		env.execute("ActivityUVCountHashSetTest");
	}
}
