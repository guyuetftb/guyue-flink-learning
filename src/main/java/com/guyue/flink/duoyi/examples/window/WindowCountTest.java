package com.guyue.flink.duoyi.examples.window;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @ClassName WindowCountTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-04 21:31
 */
public class WindowCountTest {

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

		AllWindowedStream<Integer, GlobalWindow> windowAll = map.countWindowAll(5);
		SingleOutputStreamOperator<Integer> sumDataStream = windowAll.sum(0);

		System.out.println(" All Window Stream parallel = " + sumDataStream.getParallelism());
		sumDataStream.setParallelism(2);
		sumDataStream.print();

		env.execute("WindowCountTest");
	}
}
