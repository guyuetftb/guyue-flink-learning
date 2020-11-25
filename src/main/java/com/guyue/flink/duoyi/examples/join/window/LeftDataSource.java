package com.guyue.flink.duoyi.examples.join.window;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @ClassName LeftDataSource
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-10 22:06
 */
public class LeftDataSource extends RichParallelSourceFunction<Tuple3<String, String, Long>> {

	private volatile boolean running = true;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		System.out
			.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", open()");
	}

	@Override
	public void close() throws Exception {
		super.close();
		System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", close()");
	}

	@Override
	public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
		System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", run()");

		Tuple3[] elements = new Tuple3[]{
			Tuple3.of("a", "1", 1000000050000L), // 50000-60000
			Tuple3.of("a", "2", 1000000054000L), // 50000-60000
			Tuple3.of("a", "3", 1000000079900L), // 70000-80000

			Tuple3.of("a", "4", 1000000115000L), // 110000-120000
			// type-1
			// 延迟时间设置为5000时, 115000 - 5000 = 110000 > 109999 (上一个窗口最大等待时间, 超过这个值, 窗口就会触发计算),
			// 则 [100000-109999] 或 [100000 - 110000) 这个窗口的计算将被触发,
			// 那么下面两条数据就 算是延迟了, 就不会再参与计算了, 如果不处理就会被丢掉.

			// type-2
			// 延迟时间设置为5002时, 115000 - 5002 = 109998 < 109999 (上一个窗口最大等待时间, 超过这个值, 窗口就会触发计算),
			// 显然 109998 没有达到上一个窗口的触发条件, 所以
			// 则 [100000-109999] 或 [100000 - 110000) 这个窗口 不会 触发计算,
			// 下面两条数据就能正常进入窗口期, 完成计算.

			Tuple3.of("b", "5", 1000000100000L), // 100000-110000
			Tuple3.of("b", "6", 1000000108000L)  // 100000-110000
		};

		int count = 0;
		while (running && count < elements.length) {
			System.out.println("left-source-time" + System.currentTimeMillis() + ", " + elements[count]);
			ctx.collect(Tuple3.of((String) elements[count].f0, (String) elements[count].f1, (Long) elements[count].f2));
			count++;
			Thread.sleep(1000);
		}

		// Thread.sleep(100000000);
	}

	@Override
	public void cancel() {
		System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", cancel()");
		running = false;
	}
}
