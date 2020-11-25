package com.guyue.flink.duoyi.examples.join.window;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @ClassName LeftDataSource
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-10 22:06
 */
public class RightDataSource extends RichParallelSourceFunction<Tuple3<String, String, Long>> {

	private volatile boolean running = true;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", open()");
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
			Tuple3.of("a", "杭州", 1000000059000L), // [50000, 60000)
			Tuple3.of("b", "北京", 1000000105000L)	// [100000, 110000)
		};

		int count = 0;
		while (running && count < elements.length) {
			System.out.println("right-source-time" + System.currentTimeMillis() + ", " + elements[count]);
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
