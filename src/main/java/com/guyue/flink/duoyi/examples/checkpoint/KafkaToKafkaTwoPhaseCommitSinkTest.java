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
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @ClassName RestartingStrategies
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-07 14:56
 */
public class KafkaToKafkaTwoPhaseCommitSinkTest {

	public static void main(String[] args) throws Exception {

		// BiliBili
		// https://www.bilibili.com/video/av75625853?p=68

		FlinkKafkaProducer f;
		/**
		 * FlinkKafkaProducer
		 * 1. CheckpointedFunction.initializeState
		 * 2. RichSinkFunction.invoke
		 * 3. CheckpointedFunction.snapshotState
		 * 4. preCommit
		 * 5. CheckpointListener.notifyCheckpointComplete.
		 * 6.
		 */

	}
}
