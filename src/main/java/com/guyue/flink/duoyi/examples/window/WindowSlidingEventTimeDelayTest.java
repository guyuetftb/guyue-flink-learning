package com.guyue.flink.duoyi.examples.window;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @ClassName WindowCountTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-04 21:31
 */
public class WindowSlidingEventTimeDelayTest {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 如果不写这句, 默认按照 processing_time 来处理数据
		// 即使程序中提取了数据的事件时间, 也不生效, 起作用.
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStreamSource<String> localhostSocket = env.socketTextStream("localhost", 8888);

		SingleOutputStreamOperator<String> lines = localhostSocket
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(1)) {
				@Override
				public long extractTimestamp(String element) {
					String[] fields = element.split(",");
					System.out.println(System.currentTimeMillis() + ", element=" + element);
					return Long.parseLong(fields[0]);
				}
			});

		SingleOutputStreamOperator<Tuple2<String, Integer>> map = lines.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(String value) throws Exception {
				String[] fields = value.split(",");
				String word = fields[1];
				Integer freq = Integer.parseInt(fields[2]);
				System.out.println(" word = " + word + ", freq = " + freq);
				return Tuple2.of(word, freq);
			}
		});

		// 先对数据进行分组
		KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = map.keyBy(0);

		// 滑动窗口
		WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> tumblingEventTimeWindowedStream = keyedStream
			.window(SlidingEventTimeWindows.of(Time.seconds(6),Time.seconds(2)));

		SingleOutputStreamOperator<Tuple2<String, Integer>> summed2 = tumblingEventTimeWindowedStream.sum(1);
		summed2.print("tumbling-window event-time type1-");

		env.execute("WindowSlidingEventTimeDelayTest");
	}
}
