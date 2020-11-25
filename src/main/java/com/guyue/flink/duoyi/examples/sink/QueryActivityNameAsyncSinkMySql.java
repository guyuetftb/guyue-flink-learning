package com.guyue.flink.duoyi.examples.sink;

import com.guyue.flink.duoyi.examples.dimension.bean.ActivityBean;
import com.guyue.flink.duoyi.examples.dimension.function.DataToActivityBeanAsyncMySqlFunction;
import com.guyue.flink.duoyi.examples.dyutil.DYFlinkUtilV1;
import com.guyue.flink.duoyi.examples.sink.function.MySqlSinkFunction;
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
public class QueryActivityNameAsyncSinkMySql {

	public static void main(String[] args) throws Exception {

		args = new String[]{"localhost:9092", "activity_topic", "gid-wc10-test1"};

		DataStreamSource<String> dataStreamSource = DYFlinkUtilV1.createDataStreamSource(args, new SimpleStringSchema());

		SingleOutputStreamOperator<ActivityBean> activityBeanAsyncStream = AsyncDataStream
			.unorderedWait(dataStreamSource, new DataToActivityBeanAsyncMySqlFunction(), 0, TimeUnit.MILLISECONDS, 10);

		// TODO 注意:
		// MySqlSink中的语法:
		// insert into t_activities_count (aid, event_type, `count`) values (?, ?, ?) ON DUPLICATE KEY UPDATE `count` = `count` + ?
		// 如果 MySql 语句这么写, 在 Flink 中对数据流分组, 必须加上窗口,
		// 否则, 数据结果缓存在内存, 会影响插入结果.
		// 如下面注释
		SingleOutputStreamOperator<ActivityBean> sumDataStream = activityBeanAsyncStream.keyBy("aid", "eventType").sum("count");

		sumDataStream.addSink(new MySqlSinkFunction());

		DYFlinkUtilV1.getEnv().execute("QueryActivityNameAsyncSinkMySql");
	}
}

/**
 * pool-5-thread-1, timestamp = 1583554701827, Callable.call()
 * ForkJoinPool.commonPool-worker-1, timestamp = 1583554701828, Supplier.get()
 * ForkJoinPool.commonPool-worker-1, timestamp = 1583554701914, Consumer.get()
 * Keyed Aggregation -> Sink: Unnamed (3/4), ActivityBean{uid='u001', aid='A1', activityName='新人礼物11', time='2019-09-02 10:11:11', eventType=2, province='北京市', count=1}
 * Keyed Aggregation -> Sink: Unnamed (3/4), update-number = 1
 * pool-4-thread-1, timestamp = 1583554715135, Callable.call()
 * ForkJoinPool.commonPool-worker-2, timestamp = 1583554715136, Supplier.get()
 * ForkJoinPool.commonPool-worker-2, timestamp = 1583554715160, Consumer.get()
 * Keyed Aggregation -> Sink: Unnamed (3/4), ActivityBean{uid='u001', aid='A1', activityName='新人礼物11', time='2019-09-02 10:11:11', eventType=2, province='北京市', count=2}
 * Keyed Aggregation -> Sink: Unnamed (3/4), update-number = 2
 * pool-9-thread-1, timestamp = 1583554754169, Callable.call()
 * ForkJoinPool.commonPool-worker-3, timestamp = 1583554754170, Supplier.get()
 * ForkJoinPool.commonPool-worker-3, timestamp = 1583554754195, Consumer.get()
 * Keyed Aggregation -> Sink: Unnamed (3/4), ActivityBean{uid='u001', aid='A1', activityName='新人礼物11', time='2019-09-02 10:11:11', eventType=2, province='北京市', count=3}
 * Keyed Aggregation -> Sink: Unnamed (3/4), update-number = 2
 */