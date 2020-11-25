package com.guyue.flink.duoyi.examples.window;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @ClassName WindowCountTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-04 21:31
 */
public class WindowSlidingTest {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> localhostSocket = env.socketTextStream("localhost", 8888);

		SingleOutputStreamOperator<Integer> map = localhostSocket.map(new RichMapFunction<String, Integer>() {
			@Override
			public Integer map(String value) throws Exception {
				if (StringUtils.isEmpty(value.trim())) {
					return 0;
				}
				return Integer.parseInt(value);
			}
		});

		// 窗口长度是:4
		// 2秒滑动一次
		AllWindowedStream<Integer, TimeWindow> timeWindowAllStream = map.timeWindowAll(Time.seconds(4),Time.seconds(2));
		SingleOutputStreamOperator<Integer> sumDataStream = timeWindowAllStream.sum(0);

		System.out.println(" All Window Stream parallel = " + sumDataStream.getParallelism());

		sumDataStream.print("sliding-window type1-");

		env.execute("WindowSlidingTest");
	}
}
