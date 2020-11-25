package com.guyue.flink.duoyi.examples.checkpoint;

import com.guyue.flink.duoyi.examples.dyutil.FlinkUtil;
import java.io.File;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName RestartingStrategies
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-07 14:56
 */
public class KafkaToRedisTest {

	public static void main(String[] args) throws Exception {

		ParameterTool parameterTool = ParameterTool.fromPropertiesFile(new File(args[0]));

		StreamExecutionEnvironment env = FlinkUtil.getEnv();
		String topic = parameterTool.get("kafka.topic");
		String groupId = parameterTool.get("kafka.group.id");
		DataStream<String> kafkaStream = FlinkUtil.createKafkaDataStream(parameterTool, topic, groupId, SimpleStringSchema.class);

		SingleOutputStreamOperator<Tuple3<String, String, Integer>> mapStream = kafkaStream
			.map(new RichMapFunction<String, Tuple3<String, String, Integer>>() {

				@Override
				public Tuple3<String, String, Integer> map(String line) throws Exception {
					if (line.startsWith("shutdown")) {
						throw new RuntimeException(" I am shutdown !");
					}
					return Tuple3.of("WOUND-COUNT", line.trim().toUpperCase(), 1);
				}
			});

		SingleOutputStreamOperator<Tuple3<String, String, Integer>> sumStream = mapStream.keyBy(1).sum(2);

		sumStream.addSink(new RedisSinkFunction());

		sumStream.print();

		env.execute("StateBackEndTest");
	}
}
