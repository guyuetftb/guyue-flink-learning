package com.guyue.flink.duoyi.examples.states;

import com.guyue.flink.duoyi.examples.dyutil.DYFlinkUtilV1;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
public class UserDefinedKeyedState2Test {

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

						try {
							if ("exception".equalsIgnoreCase(word)) {
								System.out.println(1 / 0);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}

						out.collect(new Tuple2<>(word, 1));
					}
				}
			});

		// keyby
		KeyedStream<Tuple2<String, Integer>, Tuple> tuple2KeyedStream = wordCountStream.keyBy(0);

		SingleOutputStreamOperator<Tuple2<String, Integer>> mapStateStream = tuple2KeyedStream
			.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

				private transient ValueState<Integer> valueState = null;

				@Override
				public void open(Configuration configuration) throws Exception {
					super.open(configuration);
					System.out.println(System.currentTimeMillis() + ", RichMapFunction.open() ");
					// guyue-wordcount-user-defined-state
					ValueStateDescriptor descriptor = new ValueStateDescriptor("gy-wc-uds", TypeInformation
						.of(new TypeHint<Tuple2<String, Integer>>() {
						})
					);
					valueState = getRuntimeContext().getState(descriptor);

					System.out.println(System.currentTimeMillis() + " " + valueState);
				}

				@Override
				public void close() throws Exception {
					super.close();
					System.out.println(System.currentTimeMillis() + ", RichMapFunction.close() ");
				}

				@Override
				public Tuple2<String, Integer> map(Tuple2<String, Integer> everyTuple) throws Exception {
					Integer oldValue = valueState.value();

					if (oldValue == null) {
						System.out.println(System.currentTimeMillis() + " oldValue = null, everyTuple = " + everyTuple);
						// 不存在, 插入
						valueState.update(everyTuple.f1);
						return Tuple2.of(everyTuple.f0, everyTuple.f1);
					} else {
						System.out.println(System.currentTimeMillis() + " oldValue = " + oldValue + ", everyTuple = " + everyTuple);
						// 不存在, 更新
						oldValue += everyTuple.f1;
						valueState.update(oldValue);
						return Tuple2.of(everyTuple.f0, oldValue);
					}
				}
			});

		mapStateStream.print();

		DYFlinkUtilV1.getEnv().execute("UserDefinedKeyedStateTest");
	}
}
