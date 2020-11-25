package com.jerome.flink.transformation;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * This is Description
 *
 * @author KONG BIN
 * @date 2019/12/11
 */
public class ReduceTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> dataStream = env.fromElements(new Tuple2("a", 3), new Tuple2("d", 4), new Tuple2("c", 2), new Tuple2("c", 5), new Tuple2("a", 5));

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = dataStream.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t2, Tuple2<String, Integer> t1) throws Exception {
                return new Tuple2<String, Integer>(t1.f0, t1.f1 + t2.f1);
            }
        });

        reduceStream.print();
        env.execute("Reduce Test");
    }
}
