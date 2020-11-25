package com.guyue.flink.duoyi.examples.window;

import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @ClassName WindowCountTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-04 21:31
 */
public class WindowSessionProcessTimeTest {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> localhostSocket = env.socketTextStream("localhost", 8888);

		SingleOutputStreamOperator<Tuple2<String, Integer>> map = localhostSocket.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(String value) throws Exception {
				String[] fields = value.split(",");
				return Tuple2.of(fields[0].trim(), Integer.parseInt(fields[1]));
			}
		});

		// 先对数据进行分组
		KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = map.keyBy(0);

		//
		WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> processingSessionWindowedStream2 = keyedStream
			.window(ProcessingTimeSessionWindows.withGap(Time.of(3, TimeUnit.SECONDS)));

		SingleOutputStreamOperator<Tuple2<String, Integer>> summed2 = processingSessionWindowedStream2.sum(1);
		summed2.print("session-window type2-");

		env.execute("WindowSessionProcessTimeTest");
	}
}
