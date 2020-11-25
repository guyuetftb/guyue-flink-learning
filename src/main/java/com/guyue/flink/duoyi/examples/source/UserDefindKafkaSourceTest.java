package com.guyue.flink.duoyi.examples.source;

import com.guyue.flink.duoyi.examples.dyutil.DYFlinkUtilV1;
import java.util.Properties;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @ClassName UserDefindParallelFileSourceTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-09 17:27
 */
public class UserDefindKafkaSourceTest {

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
		/**
		 * Kafka offset 的偏移量除了保存在 checkpoint 中外,
		 * 也提交到了 '__consumer_offset' Topic 中了，这为了以后做 Kafka Topic 消费监控使用.
		 */
		env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/checkpoint"));

		// add source
		String servers = "localhost:9092";
		String topic = "activity_topic";
		String groupId = "gid-wc10-test1";
		String offset = "earliest";

		// Kafka Source
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", servers);
		kafkaProperties.setProperty("auto.offset.reset", offset);
		kafkaProperties.setProperty("group.id", groupId);
		kafkaProperties.setProperty("enable.auto.commit", "false");

		/**
		 * Flink Checkpoint 成功后, 还要向 Kafka __consumer_offset 中写入偏移量.
		 * 此功能可以关闭: stringFlinkKafkaConsumer.setCommitOffsetsOnCheckpoints(false);
		 */
		FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), kafkaProperties);
		stringFlinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true);


		env.addSource(stringFlinkKafkaConsumer).print();
		env.execute("UserDefindKafkaSourceTest");
	}
}
