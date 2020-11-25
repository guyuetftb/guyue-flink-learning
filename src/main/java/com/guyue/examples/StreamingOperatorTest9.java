package com.guyue.examples;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by lipeng
 * com.guyue.flink
 * lipeng
 * 2019/3/18
 */
public class StreamingOperatorTest9 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Long>> srcStream = env.fromElements(
                new Tuple2<String, Long>("hello", 1l),
                new Tuple2<String, Long>("world", 1l),
                new Tuple2<String, Long>("hello", 1l),
                new Tuple2<String, Long>("world", 1l),
                new Tuple2<String, Long>("other", 1l)
        );

        System.out.println("----------------- keyBy() ");
        KeyedStream<Tuple2<String, Long>, String> keyedStream = srcStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {

            @Override
            public String getKey(Tuple2<String, Long> t1) throws Exception {
                return t1.f0;
            }
        });

        System.out.println("----------------- reduce() ");
        DataStream<Tuple2<String, Long>> reduceStram = keyedStream.reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> t1,
                                               Tuple2<String, Long> t2) throws Exception {
                return new Tuple2<String, Long>(t1.f0, t1.f1 + t2.f1);
            }
        });

        System.out.println("----------------- fold() ");
//        DataStream<Tuple2<String,Long>> foldStrem = keyedStream.fold(String , new FoldFunction<Tuple2<String, Long>, Tuple2<String,Long>>() {
//
//            @Override
//            public Tuple2<String, Long> fold(String oldVal,
//                                             Tuple2<String, Long> t2) throws Exception {
//                return new Tuple2<String,Long>(src.f0, (src.f1 + t2.f1));
//            }
//        });



    }
}
