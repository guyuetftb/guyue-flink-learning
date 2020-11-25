package com.guyue.flink.duoyi.examples.dyutil;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV2Serializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @ClassName DYFlinkUtilV1
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-06 18:01
 */
public class FlinkUtil {

	private static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


	public static <T> DataStream<T> createKafkaDataStream(ParameterTool parameterTool, String topic, String gid,
														  Class<? extends DeserializationSchema<T>> clazz)
		throws IllegalAccessException, InstantiationException {

		// Kafka property
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", parameterTool.get("kafka.bootstrap.servers", "localhost:9092"));
		kafkaProperties.setProperty("auto.offset.reset", parameterTool.get("kafka.offset", "earliest"));
		kafkaProperties.setProperty("enable.auto.commit", parameterTool.get("kafka.auto.commit", "false"));
		kafkaProperties.setProperty("group.id", gid);
		List<String> topics = Arrays.asList(topic.split(","));

		// FlinkKafkaConsumer
		FlinkKafkaConsumer<T> flinkKafkaConsumer = new FlinkKafkaConsumer<T>(topics, clazz.newInstance(), kafkaProperties);
		flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true);

		return env.addSource(flinkKafkaConsumer);
	}

	public static void settingCheckpoint() {

		CheckpointConfig checkpointConfig = env.getCheckpointConfig();

		// checkpoint 间隔
		checkpointConfig.setCheckpointInterval(5000);

		// checkpoint 超时时间
		checkpointConfig.setCheckpointTimeout(2000);

		// 任务失败 checkpoint 策略
		checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		// checkpoint 机制
		checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
	}


	public static StreamExecutionEnvironment getEnv() {
		return env;
	}
}
