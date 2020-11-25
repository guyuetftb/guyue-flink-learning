package com.guyue.flink.duoyi.examples.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName PrintSinkTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-03 23:12
 */
public class PrintSinkTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> localhostSocket = env.socketTextStream("localhost", 8888);
		System.out.println(" Socket parallel = " + localhostSocket.getParallelism());

		localhostSocket.print("++++++>").setParallelism(2);

		env.execute("PrintSinkTest");
	}
}
