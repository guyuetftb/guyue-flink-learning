package com.jerome.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * This is Description
 *
 * @author Jerome丶子木
 * @date 2019/12/25
 */
public class ValueStateTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Integer, Long>> inputStream = env.fromElements(new Tuple2<Integer, Long>(2, 21L), new Tuple2<Integer, Long>(4, 1L), new Tuple2<Integer, Long>(5, 4L));

        SingleOutputStreamOperator<Tuple3<Integer, Long, Long>> leastValue = inputStream.keyBy(0).flatMap(new RichFlatMapFunction<Tuple2<Integer, Long>, Tuple3<Integer, Long, Long>>() {

            private ValueState<Long> leastValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //创建ValueStateDescriptor，定意状态名称为leastValue，并指定数据类型
                ValueStateDescriptor<Long> leastValueStateDescriptor = new ValueStateDescriptor<>("leastValue", Long.class);
                //通过getRuntimeContext.getState获取State
                leastValueState = getRuntimeContext().getState(leastValueStateDescriptor);
            }

            @Override
            public void flatMap(Tuple2<Integer, Long> value, Collector<Tuple3<Integer, Long, Long>> out) throws Exception {
                //通过value方法从leastValueState中获取最小值
                Long leastValue = leastValueState.value();

                if (leastValue != null) {
                    //如果当前指标大于最小值，则直接输出数据元素和最小值
                    if (value.f1 > leastValue) {
                        out.collect(new Tuple3<>(value.f0, value.f1, leastValue));
                    } else {
                        //如果当前指标小于最小值，则更新状态中的最小值
                        leastValueState.update(value.f1);
                        //将当前数据中的指标作为最小值输出
                        out.collect(new Tuple3<>(value.f0, value.f1, value.f1));
                    }
                }else {
                    //如果状态中的值是空，则更新状态中的最小值
                    leastValueState.update(value.f1);
                    //将当前数据中的指标作为最小值输出
                    out.collect(new Tuple3<>(value.f0, value.f1, value.f1));
                }

            }
        });

        leastValue.print();

        env.execute("ValueStateTest");

    }
}
