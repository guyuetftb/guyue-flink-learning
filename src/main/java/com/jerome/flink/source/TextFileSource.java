package com.jerome.flink.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * This is Description
 *
 * @author Jerome丶子木
 * @date 2019/12/19
 */
public class TextFileSource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.readTextFile(args[0]);

        int parallelism = lines.getParallelism();

        System.out.println("source parallelism is =============> " + parallelism);

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMaped = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] splits = value.split(",");
                for (String split : splits) {
                    out.collect(Tuple2.of(split, 1));

                }
            }
        });

        int parallelism1 = flatMaped.getParallelism();

        System.out.println("transformation parallelism is =============> " + parallelism1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = flatMaped.keyBy(0).sum(1);

        int parallelism2 = sum.getParallelism();
        System.out.println("transformation parallelism is =============> " + parallelism2);
        sum.print();

        env.execute("TextFileSource");

    }
}
