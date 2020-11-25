package com.guyue.flink.duoyi.examples;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

/**
 * @ClassName SourceParallelTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-02 22:40
 */
public class SourceParallelTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 					SplittableIterator
		// 					|				|
		// NumberSequenceIterator		LongValueSequenceIterator

		// method-1
		// DataStream<Long> nums = env.fromParallelCollection(new NumberSequenceIterator(1,100), TypeInformation.of(Long.class));

		// method-2
		DataStream<Long> nums = env.fromParallelCollection(new NumberSequenceIterator(1, 20), Types.LONG);

		// 默认情况下, 并行度等于当前机器可用的 "逻辑核数", 逻辑核数不是物理核数, 2核4线程, 逻辑核等于4.
		int parallelism = nums.getParallelism();

		System.out.println("++++++++++++++++++> parallelism = " + parallelism);

		DataStream<Long> evens = nums.filter(new FilterFunction<Long>() {
			@Override
			public boolean filter(Long value) throws Exception {

				return value % 2 == 0;
			}
		}).setParallelism(3);

		int parallelism1 = evens.getParallelism();

		System.out.println("==================> parallelism1 = " + parallelism1);
		evens.print();

		env.execute("SourceParallelTest");

	}
}

/**
 * ++++++++++++++++++> parallelism = 4
 * ==================> parallelism1 = 3
 * 23:15:40,779 WARN  org.apache.flink.runtime.webmonitor.WebMonitorUtils           - Log file environment variable 'log.file' is not set.
 * 23:15:40,781 WARN  org.apache.flink.runtime.webmonitor.WebMonitorUtils           - JobManager log files are unavailable in the web dashboard. Log file location not found in environment variable 'log.file' or configuration key 'Key: 'web.log.path' , default: null (fallback keys: [{key=jobmanager.web.log.path, isDeprecated=true}])'.
 * 4> 18
 * 1> 8
 * 3> 16
 * 1> 6
 * 2> 4
 * 4> 14
 * 1> 10
 * 4> 12
 * 2> 2
 * 3> 20
 */
