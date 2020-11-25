package com.guyue.flink.duoyi.examples.join.apps;

import com.alibaba.fastjson.JSON;
import com.guyue.flink.duoyi.examples.dyutil.FlinkUtil;
import com.guyue.flink.duoyi.examples.join.apps.bean.OrderDetail;
import com.guyue.flink.duoyi.examples.join.apps.bean.OrderMain;
import java.io.File;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 1. 使用 EventTime 划分滚动窗口 2. 使用 CoGroup 实时左外连接 3. 没有 Join 上的数据单独处理 4.
 *
 * @ClassName OrderJoin
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-10 22:07
 */
public class OrderInnerJoinProcessFunctionTest {


	public static void main(String[] args) throws Exception {

		// 左表数据 Join 右表数据, 右表迟到没有 Join 上, 直接查询数据库.
		// 左表数据迟到, 使用测输出.
		StreamExecutionEnvironment env = FlinkUtil.getEnv();
		ExecutionConfig envConfig = env.getConfig();
		envConfig.setParallelism(1);

		// global configuration
		String configPath = "/Users/lipeng/workspace_link/streamlink/streamlink-engine/streamlink-flink-learning/src/main/data/config.properties";
		ParameterTool parameterTool = ParameterTool.fromPropertiesFile(new File(configPath));
		envConfig.setGlobalJobParameters(parameterTool);

		// parameters
		String streamTimeCharacteristic = parameterTool.get("stream.time.characteristic");
		long windowSize = parameterTool.getInt("tumbling.window.size");
		int delayTime = parameterTool.getInt("window.delay.time");
		String localCheckpointPath = parameterTool.get("local.checkpoint.path");
		String checkpointMode = parameterTool.get("checkpoint.mode");
		long checkpointInterval = parameterTool.getInt("checkpoint.interval");
		String checkpointExternalized = parameterTool.get("checkpoint.externalized");

		// stream configuration.
		env.setStateBackend(new FsStateBackend(localCheckpointPath));
		env.setStreamTimeCharacteristic(TimeCharacteristic.valueOf(streamTimeCharacteristic));
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2000, 1000));

		// checkpint
		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.setCheckpointInterval(checkpointInterval);
		checkpointConfig.setCheckpointingMode(CheckpointingMode.valueOf(checkpointMode));
		checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.valueOf(checkpointExternalized));

		// create order main stream
		String orderMainTopic = parameterTool.get("kafka.topic.order.main");
		String orderMainTopicGid = parameterTool.get("kafka.topic.order.main.gid");

		SingleOutputStreamOperator<OrderMain> orderMainStream = FlinkUtil
			.createKafkaDataStream(parameterTool, orderMainTopic, orderMainTopicGid, SimpleStringSchema.class)
			.process(new ProcessFunction<String, OrderMain>() {
				@Override
				public void processElement(String line, Context ctx, Collector<OrderMain> collector) throws Exception {
					System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", order_main-process");
					OrderMain orderMain = JSON.parseObject(line, OrderMain.class);
					collector.collect(orderMain);
				}
			})
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderMain>(Time.milliseconds(delayTime)) {
				@Override
				public long extractTimestamp(OrderMain orderMain) {
					System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", order_main-watermark");
					return orderMain.getUpdateTime().getTime();
				}
			});

		// create order detail stream
		String orderDetailTopic = parameterTool.get("kafka.topic.order.detail");
		String orderDetailTopicGid = parameterTool.get("kafka.topic.order.detail.gid");
		SingleOutputStreamOperator<OrderDetail> orderDetailStream = FlinkUtil
			.createKafkaDataStream(parameterTool, orderDetailTopic, orderDetailTopicGid, SimpleStringSchema.class)
			.process(new ProcessFunction<String, OrderDetail>() {
				@Override
				public void processElement(String line, Context ctx, Collector<OrderDetail> collector) throws Exception {
					System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", order_detail-process");
					OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
					collector.collect(orderDetail);
				}
			})
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderDetail>(Time.milliseconds(delayTime)) {
				@Override
				public long extractTimestamp(OrderDetail orderDetail) {
					System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis() + ", order_detail-watermark");
					return orderDetail.getUpdateTime().getTime();
				}
			});

		DataStream<Tuple2<OrderDetail, OrderMain>> orderJoinedStream = orderDetailStream.coGroup(orderMainStream)
			.where(new OrderDetailKeySelector())
			.equalTo(new OrderMainKeySelector())
			.window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
			.apply(new OrderJoinCoGroupFunction());

		orderJoinedStream.print("order-joined-----");

		env.execute("OrderInnerJoinProcessFunctionTest");
	}

	public static class OrderMainKeySelector implements KeySelector<OrderMain, Long> {

		@Override
		public Long getKey(OrderMain orderMain) throws Exception {
			System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis()
				+ ", order_main_key=" + orderMain.getOrderId());
			return orderMain.getOrderId();
		}
	}

	public static class OrderDetailKeySelector implements KeySelector<OrderDetail, Long> {

		@Override
		public Long getKey(OrderDetail orderDetail) throws Exception {
			System.out.println(Thread.currentThread().getName() + ", time=" + System.currentTimeMillis()
				+ ", order_detail_key=" + orderDetail.getOrderId());
			return orderDetail.getOrderId();
		}
	}

	public static class OrderJoinCoGroupFunction implements CoGroupFunction<OrderDetail, OrderMain, Tuple2<OrderDetail, OrderMain>> {


		@Override
		public void coGroup(Iterable<OrderDetail> firstIterator, Iterable<OrderMain> secondIterator,
							Collector<Tuple2<OrderDetail, OrderMain>> collector)
			throws Exception {

			for (OrderDetail orderDetail : firstIterator) {

				boolean isJoined = false;

				for (OrderMain orderMain : secondIterator) {

					if (orderDetail.getOrderId().equals(orderMain.getOrderId())) {
						isJoined = true;
						collector.collect(Tuple2.of(orderDetail.clone(), orderMain.clone()));
					}
				}

				if (!isJoined) {
					// 如果 join 不上, 赋一空 OrderMain 对象.
					collector.collect(Tuple2.of(orderDetail.clone(), new OrderMain()));
				}
			}

		}
	}
}
