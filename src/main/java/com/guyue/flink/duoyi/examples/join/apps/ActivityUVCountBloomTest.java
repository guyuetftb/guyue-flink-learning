package com.guyue.flink.duoyi.examples.join.apps;

import com.guyue.flink.duoyi.examples.dimension.bean.ActivityBean;
import com.guyue.flink.duoyi.examples.dyutil.FlinkUtil;
import java.io.File;
import java.util.HashSet;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnel;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
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
public class ActivityUVCountBloomTest {

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

		checkpointConfig.setCheckpointTimeout(1000);

		// 同一时间，只允许 有 1 个 Checkpoint 在发生
		checkpointConfig.setMaxConcurrentCheckpoints(1);

		// 两次 Checkpoint 之间的最小时间间隔为 500 毫秒
		checkpointConfig.setMinPauseBetweenCheckpoints(500);

		// 当有较新的 Savepoint 时，作业也会从 Checkpoint 处恢复
		checkpointConfig.setPreferCheckpointForRecovery(true);

		// 作业最多允许 Checkpoint 失败 1 次（flink 1.9 开始支持）
		checkpointConfig.setTolerableCheckpointFailureNumber(1);

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

				private transient ValueState<BloomFilter> uidState = null;
				private transient ValueState<Integer> uidCountState = null;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					ValueStateDescriptor<BloomFilter> uidDescriptor = new ValueStateDescriptor("uid-set", BloomFilter.class);
					uidState = getRuntimeContext().getState(uidDescriptor);

					ValueStateDescriptor<Integer> uidCountDescriptor = new ValueStateDescriptor("uid-count", Integer.class);
					uidCountState = getRuntimeContext().getState(uidCountDescriptor);
				}

				@Override
				public void close() throws Exception {
					super.close();
					uidState.clear();
					uidCountState.clear();
				}

				@Override
				public Tuple3<String, Integer, Integer> map(ActivityBean activityBean) throws Exception {
					BloomFilter bloomFilter = uidState.value();
					Integer uidCount = uidCountState.value();
					if (null == bloomFilter) {
						bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 10000000);
						uidCount = 0;
					}

					// Bloom 过滤器可以判断一定不包含.
					boolean mightContainUid = bloomFilter.mightContain(activityBean.uid);
					if (!mightContainUid) {
						bloomFilter.put(activityBean.uid);
						uidCount += 1;
					}

					uidState.update(bloomFilter);
					uidCountState.update(uidCount);

					return Tuple3.of(activityBean.aid, activityBean.eventType, uidCount);
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

		activityGroup.print("uid-uv-count-bloom-");

		env.execute("ActivityUVCountBloomTest");
	}
}
