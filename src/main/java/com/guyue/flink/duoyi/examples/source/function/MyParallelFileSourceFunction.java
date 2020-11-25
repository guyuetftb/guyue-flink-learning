package com.guyue.flink.duoyi.examples.source.function;

import java.io.RandomAccessFile;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @ClassName MyParallelFileSourceFunction
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-09 17:12
 */
public class MyParallelFileSourceFunction extends RichParallelSourceFunction<Tuple2<String, String>> {

	private int subtaskIndex = -1;

	private String path;

	private MessageAcknowledgingSourceBase a;

	private boolean flag = true;

	public MyParallelFileSourceFunction(String path) {
		this.path = path;
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		System.out.println(System.currentTimeMillis() + ", RichParallelSourceFunction.open() ");
		subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
	}

	@Override
	public void close() throws Exception {
		super.close();
		System.out.println(System.currentTimeMillis() + ", RichParallelSourceFunction.close() ");
	}

	@Override
	public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
		System.out.println(System.currentTimeMillis() + ", RichParallelSourceFunction.run() ");

		String fileName = path + "/" + subtaskIndex + ".txt";
		RandomAccessFile randomAccessFile = new RandomAccessFile(fileName, "r");

		while (flag) {
			String line = randomAccessFile.readLine();
			if (StringUtils.isEmpty(line)) {
				Thread.sleep(500);
			} else {
				String lineUTF8 = new String(line.getBytes("ISO-8859-1"), "utf-8");
				ctx.collect(Tuple2.of(subtaskIndex + "", lineUTF8));
			}
		}
	}

	@Override
	public void cancel() {
		flag = false;
	}
}
