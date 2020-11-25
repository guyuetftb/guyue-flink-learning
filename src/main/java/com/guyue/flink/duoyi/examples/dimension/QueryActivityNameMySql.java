package com.guyue.flink.duoyi.examples.dimension;

import com.guyue.flink.duoyi.examples.dimension.bean.ActivityBean;
import com.guyue.flink.duoyi.examples.dimension.function.DataToActivityBeanMySqlFunction;
import com.guyue.flink.duoyi.examples.dyutil.DYFlinkUtilV1;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * @ClassName QueryActivityNameMySql
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-06 18:02
 */
public class QueryActivityNameMySql {

	public static void main(String[] args) throws Exception {

		args = new String[]{"localhost:9092", "activity_topic", "gid-wc10-test1"};

		DataStreamSource<String> dataStreamSource = DYFlinkUtilV1.createDataStreamSource(args, new SimpleStringSchema());

		SingleOutputStreamOperator<ActivityBean> activityMapStream = dataStreamSource.map(new DataToActivityBeanMySqlFunction());

		activityMapStream.print();

		DYFlinkUtilV1.getEnv().execute("QueryActivityNameMySql");
	}
}
