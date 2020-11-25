package com.guyue.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountSocket {

	public static void main(String[] args) {

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

		// the port to connect to
		String host = null;
		int port = 0;
		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			host = params.get("host");
			port = params.getInt("port");
		} catch (Exception e) {
			System.err.println("Please run 'WordCountSocket --host <ip> --port <port>'");
			System.exit(1);
		}

		DataStream<Tuple2<String, Integer>> dataStream = see
			.socketTextStream(host, port, "\n")     // 接入 Socket 输入
			.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

				@Override
				public void flatMap(String sentence,
									Collector<Tuple2<String, Integer>> collector) throws Exception {
					String[] arr = sentence.split("\\s");
					for (String s : arr) {
						if (s.isEmpty()) {
							continue;
						}
						collector.collect(new Tuple2(s.trim(), 1));
					}
				}
			})  // 把单词按空字符分隔
			.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

				@Override
				public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
					return tuple2.f0;
				}
			})  // 根据 Key 值分组
			.timeWindow(Time.seconds(5))    // 5秒一个窗口
			.sum(1);    // 根据word来汇总

		// 将内容输出到 标准输出, 设置并行度为1
		dataStream.print().setParallelism(1);

		try {
			see.execute("Socket Stream WordCount");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
