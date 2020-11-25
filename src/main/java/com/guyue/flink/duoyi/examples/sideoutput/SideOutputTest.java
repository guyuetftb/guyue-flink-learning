package com.guyue.flink.duoyi.examples.sideoutput;

import com.guyue.flink.duoyi.examples.dyutil.FlinkUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName SideOutputTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-14 14:16
 */
public class SideOutputTest {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = FlinkUtil.getEnv();

		DataStreamSource<String> localhostSocketStream = env.socketTextStream("localhost", 8888);

		final OutputTag<String> less5Word = new OutputTag<String>("less5Word"){};
		SingleOutputStreamOperator<String> great5Stream = localhostSocketStream.process(new ProcessFunction<String, String>() {
			@Override
			public void processElement(String line, Context context, Collector<String> collector) throws Exception {
				String[] words = line.split("\\W+");
				for (String word : words) {
					if (word.length() < 5) {
						context.output(less5Word, word);
					} else {
						collector.collect(word);
					}
				}
			}
		});

		great5Stream.print("great-5-");

		DataStream<String> less5Stream = great5Stream.getSideOutput(less5Word);
		less5Stream.print("less-5-");

		env.execute("SideOutputTest");

	}
}
