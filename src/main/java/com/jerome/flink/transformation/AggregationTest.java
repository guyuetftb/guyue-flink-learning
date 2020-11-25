package com.jerome.flink.transformation;

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
public class AggregationTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Integer, Integer>> dataStream = env.fromElements(new Tuple2<Integer, Integer>(1, 5), new Tuple2<Integer, Integer>(2, 2), new Tuple2<Integer, Integer>(2, 4), new Tuple2<Integer, Integer>(1, 3));

        KeyedStream<Tuple2<Integer, Integer>, Tuple> keyedStream = dataStream.keyBy(0);

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> sum = keyedStream.sum(1);

        sum.print();

        env.execute("sum test");


    }
}
