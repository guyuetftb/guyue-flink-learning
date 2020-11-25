package com.jerome.flink.dataset;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * This is Description
 *
 * @author Jerome丶子木
 * @date 2019/12/30
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> text = env.fromElements("who's there?", "Hello World!");

        AggregateOperator<Tuple2<String, Integer>> sum = text.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.toLowerCase().split("\\W+");

                for (String word :
                        split) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }).filter(new RichFilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                return null != value;
            }
        }).groupBy(0)
                .sum(1);

        sum.print();
    }
}
