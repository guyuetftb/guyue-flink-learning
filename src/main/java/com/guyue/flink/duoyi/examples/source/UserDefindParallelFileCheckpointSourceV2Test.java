package com.guyue.flink.duoyi.examples.source;

import com.guyue.flink.duoyi.examples.dyutil.DYFlinkUtilV1;
import com.guyue.flink.duoyi.examples.source.function.MyParallelFileSourceCheckpointFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName UserDefindParallelFileSourceTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-09 17:27
 */
public class UserDefindParallelFileCheckpointSourceV2Test {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = DYFlinkUtilV1.getEnv();

		// restart strategy
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2000));

		// checkpoint
		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.setCheckpointInterval(5000);
		checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		// backend
		env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/checkpoint"));

		// add source
		String path = "/Users/lipeng/workspace_link/streamlink/streamlink-engine/streamlink-flink-learning/src/main/data/source";
		DataStreamSource<Tuple2<String, String>> userDefindStream = env
			.addSource(new MyParallelFileSourceCheckpointFunction(path));

		// version-1
		userDefindStream.print();

		DataStreamSource<String> localhostSocket = env.socketTextStream("localhost", 8888);
		SingleOutputStreamOperator<String> socketStream = localhostSocket.map(new RichMapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				if (value.startsWith("exception")) {
					throw new RuntimeException(" 抛出异常, 测试!");
				}
				return value;
			}
		});
		socketStream.print("socket-");

		env.execute("UserDefindParallelFileSourceTest");
	}
}
