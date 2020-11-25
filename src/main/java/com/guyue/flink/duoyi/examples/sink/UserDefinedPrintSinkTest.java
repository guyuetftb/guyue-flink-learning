package com.guyue.flink.duoyi.examples.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @ClassName PrintSinkTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-03 23:12
 */
public class UserDefinedPrintSinkTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> localhostSocket = env.socketTextStream("localhost", 8888);
		System.out.println(" Socket parallel = " + localhostSocket.getParallelism());

		// 自定义 Sink
		DataStreamSink<String> stringDataStreamSink = localhostSocket.addSink(new RichSinkFunction<String>() {
			@Override
			public void open(Configuration parameters) throws Exception {
				System.out.println("RichSinkFunction ++++++++:> open() " + Thread.currentThread().getName());
			}

			@Override
			public void close() throws Exception {
				System.out.println("RichSinkFunction ++++++++:> close() " + Thread.currentThread().getName());
			}

			@Override
			public void invoke(String value, Context context) throws Exception {
				// AbstractRichSinkFunction 中的方法
				int thisSubtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
				System.out.println(thisSubtaskIndex + " :> " + value);
			}
		}).name("UserDefinedSink");


		env.execute("UserDefinedPrintSinkTest");
	}
}
