package com.jerome.flink.transformation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

/**
 * This is Description
 *
 * @author KONG BIN
 * @date 2019/12/11
 */
public class ConnectTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> dataStream = env.fromElements(new Tuple2<String, Integer>("a", 2), new Tuple2<>("b", 4));
        DataStreamSource<Integer> dataStream2 = env.fromElements(1, 2);

        ConnectedStreams<Tuple2<String, Integer>, Integer> connectedStreams = dataStream.connect(dataStream2);

//        SingleOutputStreamOperator<Tuple2<Integer, String>> outputStreamOperator = connectedStreams.map(new CoMapFunction<Tuple2<String, Integer>, Integer, Tuple2<Integer, String>>() {
//            @Override
//            public Tuple2<Integer, String> map1(Tuple2<String, Integer> tuple2) throws Exception {
//                return new Tuple2<>(tuple2.f1, tuple2.f0);
//            }
//
//            @Override
//            public Tuple2<Integer, String> map2(Integer integer) throws Exception {
//                return new Tuple2<>(integer, "default");
//            }
//        });
//
//        outputStreamOperator.print();

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> out = connectedStreams.flatMap(new CoFlatMapFunction<Tuple2<String, Integer>, Integer, Tuple3<String, Integer, Integer>>() {

            int number = 0;

            @Override
            public void flatMap1(Tuple2<String, Integer> tuple2, Collector<Tuple3<String, Integer, Integer>> collector) throws Exception {

                collector.collect(new Tuple3<>(tuple2.f0, tuple2.f1, number));

            }

            @Override
            public void flatMap2(Integer integer, Collector<Tuple3<String, Integer, Integer>> collector) throws Exception {
                number = integer;

            }
        });

        out.print();


        env.execute("connect test");


    }
}
