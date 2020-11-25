package com.jerome.flink.transformation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * This is Description
 *
 * @author KONG BIN
 * @date 2019/12/11
 */
public class UnionTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> datastream1 = env.fromElements(new Tuple2<String, Integer>("a", 3), new Tuple2<String, Integer>("d", 4), new Tuple2<String, Integer>("c", 2), new Tuple2<String, Integer>("c", 5), new Tuple2<String, Integer>("a", 5));
        DataStreamSource<Tuple2<String, Integer>> datastream2 = env.fromElements(new Tuple2<String, Integer>("d", 1), new Tuple2<String, Integer>("s", 2), new Tuple2<String, Integer>("a", 4), new Tuple2<String, Integer>("e", 5), new Tuple2<String, Integer>("a", 6));
        DataStreamSource<Tuple2<String, Integer>> datastream3 = env.fromElements(new Tuple2<String, Integer>("a", 2), new Tuple2<String, Integer>("d", 1), new Tuple2<String, Integer>("s", 2), new Tuple2<String, Integer>("c", 3), new Tuple2<String, Integer>("b", 1));

        DataStream<Tuple2<String, Integer>> union = datastream1.union(datastream2);

//        DataStream<Tuple2<String, Integer>> union1 = datastream1.union(datastream2, datastream3);
        union.print();

        env.execute("union test");


    }
}
