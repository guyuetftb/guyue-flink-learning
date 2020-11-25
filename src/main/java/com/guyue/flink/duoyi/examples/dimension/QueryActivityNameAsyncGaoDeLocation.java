package com.guyue.flink.duoyi.examples.dimension;

import com.guyue.flink.duoyi.examples.dimension.bean.ActivityBean;
import com.guyue.flink.duoyi.examples.dimension.function.DataToActivityBeanAsyncGaoDeFunction;
import com.guyue.flink.duoyi.examples.dyutil.DYFlinkUtilV1;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * @ClassName QueryActivityNameMySql
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-06 18:02
 */
public class QueryActivityNameAsyncGaoDeLocation {

	public static void main(String[] args) throws Exception {

		args = new String[]{"localhost:9092", "activity_topic", "gid-wc10-test1"};

		DataStreamSource<String> dataStreamSource = DYFlinkUtilV1.createDataStreamSource(args, new SimpleStringSchema());

		// 在设置容量的时候,不能超过, 异步 function 的最大连接数量.
		SingleOutputStreamOperator<ActivityBean> activityBeanAsyncStream = AsyncDataStream
			.orderedWait(dataStreamSource, new DataToActivityBeanAsyncGaoDeFunction(), 1000, TimeUnit.MILLISECONDS, 10);

		activityBeanAsyncStream.print();

		DYFlinkUtilV1.getEnv().execute("QueryActivityNameAsyncGaoDeLocation");
	}
}
