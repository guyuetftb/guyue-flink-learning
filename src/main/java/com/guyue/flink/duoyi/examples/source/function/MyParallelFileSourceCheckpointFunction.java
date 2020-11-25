package com.guyue.flink.duoyi.examples.source.function;

import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Iterator;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.PartitionableListState;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @ClassName MyParallelFileSourceCheckpointFunction
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-09 17:12
 */
public class MyParallelFileSourceCheckpointFunction extends RichParallelSourceFunction<Tuple2<String, String>> implements
	CheckpointedFunction {

	private int subtaskIndex = -1;

	private String path;

	private boolean flag = true;

	private long offset;

	// TODO 重点, listState 必须用 transient 修饰
	private transient ListState<Long> listState;

	public MyParallelFileSourceCheckpointFunction() {
	}

	public MyParallelFileSourceCheckpointFunction(String path) {
		this.path = path;
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		System.out.println(Thread.currentThread().getName() + ", " + System.currentTimeMillis() + ", MyParallelFileSourceCheckpointFunction.open() ");
		subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
	}

	@Override
	public void close() throws Exception {
		super.close();
		System.out.println(Thread.currentThread().getName() + ", " + System.currentTimeMillis() + ",  MyParallelFileSourceCheckpointFunction.close() ");
	}

	@Override
	public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
		System.out.println(Thread.currentThread().getName() + ", " + System.currentTimeMillis() + ", MyParallelFileSourceCheckpointFunction.run() ");

		// 读取 offset 值
		// 状态数据中可以保存多个数据, 所以 ListState.get() 方法返回一个迭代器
		// 我们这里只有一个文件偏移量, 所以只读取一次就可以
		Iterator<Long> iterator = listState.get().iterator();
		while (iterator.hasNext()) {
			offset = iterator.next();
		}

		// 构造文件目录
		String fileName = path + "/" + subtaskIndex + ".txt";

		// 创建读取器, 设置偏移
		RandomAccessFile randomAccessFile = new RandomAccessFile(fileName, "r");
		randomAccessFile.seek(offset);

		// 获取锁对象
		final Object checkpointLock = ctx.getCheckpointLock();

		while (flag) {
			String line = randomAccessFile.readLine();
			if (StringUtils.isEmpty(line)) {
				Thread.sleep(500);
			} else {
				// 转码
				String lineUTF8 = new String(line.getBytes("ISO-8859-1"), "utf-8");

				// 保存文件偏移
				synchronized (checkpointLock) {
					// 发送数据
					ctx.collect(Tuple2.of(subtaskIndex + "", lineUTF8));

					offset = randomAccessFile.getFilePointer();
				}
			}
		}
	}

	@Override
	public void cancel() {
		System.out.println(Thread.currentThread().getName() + ", " + System.currentTimeMillis() + ", MyParallelFileSourceCheckpointFunction.cancel() ");
		flag = false;
	}

	/**
	 * 将 State 的状态, 定期的保存到 StateBackEnd 中。
	 */
	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		System.out.println(
			Thread.currentThread().getName() + ", " + System.currentTimeMillis() + ", MyParallelFileSourceCheckpointFunction.snapshotState() offset = " + offset);
		// 清除历史值
		// PartitionableListState
		listState.clear();

		// 更新最新的状态值
		listState.update(Collections.singletonList(offset));
	}

	/**
	 * 初始化 Checkpoint 状态, 生命周期方法, 构造器执行完后, 会执行一次
	 */
	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		System.out.println(Thread.currentThread().getName() + ", " + System.currentTimeMillis() + ", MyParallelFileSourceCheckpointFunction.initializeState()");

		// 定义一个状态描述器

		// 根据状态描述器, 初始化, 或 获取历史状态
		// 从 getRuntimeContext() 中获取的 State 数据, 都是 KeyedState.
		// 从 FunctionInitializationContext 中获取的 State 才是 OperatorState.
		OperatorStateStore operatorStateStore = context.getOperatorStateStore();

		// 创建一个状态描述器
		ListStateDescriptor listStateDescriptor = new ListStateDescriptor("file-offset",
			// type-1
			// TypeInformation.of(new TypeHint<Long>()) {}

			// type-2
			// Long.class

			// type-3
			Types.LONG
		);

		// 首次启动时, list-state 不是 null, 是 一个初始化的对象, 内容为空
		listState = operatorStateStore.getListState(listStateDescriptor);
		System.out.println(
			Thread.currentThread().getName() + ", " + System.currentTimeMillis() + ", MyParallelFileSourceCheckpointFunction.initializeState() listState = "
				+ listState);

	}
}
