package com.guyue.flink.duoyi.examples.states;

import com.guyue.flink.duoyi.examples.dimension.bean.ActivityBean;
import com.guyue.flink.duoyi.examples.dimension.function.DataToActivityBeanMySqlFunction;
import com.guyue.flink.duoyi.examples.dyutil.DYFlinkUtilV1;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendBuilder;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName QueryActivityNameMySql
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-06 18:02
 */
public class KeyedStateAndOperatorStateTest {

	public static void main(String[] args) throws Exception {

		args = new String[]{"localhost:9092", "state_topic", "gid-wc10-test1"};

		StreamExecutionEnvironment env = DYFlinkUtilV1.getEnv();
		env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/checkpoint"));

		// execution configuration
		ExecutionConfig config = env.getConfig();

		// checkpoint configuration

		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.setCheckpointInterval(5000);
		checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		// 为了实现 EXACTLY_ONCE 需要记录 Partition 偏移量
		checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		// data source
		DataStreamSource<String> dataStreamSource = DYFlinkUtilV1.createDataStreamSource(args, new SimpleStringSchema());

		SingleOutputStreamOperator<Tuple2<String, Integer>> wordCountStream = dataStreamSource.flatMap(
			new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
				@Override
				public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
					String[] split = value.toLowerCase().split("\\W+");
					for (String word : split) {
						if (StringUtils.isEmpty(word)) {
							continue;
						}
						out.collect(new Tuple2<>(word, 1));
					}
				}
			});

		// keyby
		SingleOutputStreamOperator<Tuple2<String, Integer>> wordCountSum = wordCountStream.keyBy(0).sum(1);

		wordCountSum.print();

		DYFlinkUtilV1.getEnv().execute("KeyedStateAndOperatorStateTest");
	}
}
