package com.guyue.examples;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by lipeng
 * com.guyue.flink
 * lipeng
 * 2019/3/18
 */
public class StreamingIteration7 {
    public static void main(String[] args) {
        // timeoutMillis 默认值是 100 毫秒 [millisecond]
        long bufferTimeOut = 200;
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(bufferTimeOut);

        DataStream<Long> someIntegers = env.generateSequence(-10, 100);
        IterativeStream<Long> iteration = someIntegers.iterate();

        DataStream<Long> minusOne = iteration.map(new MapFunction<Long,Long>(){

            @Override
            public Long map(Long num) throws Exception {
                return num -1 ;
            }
        });

        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {

            @Override
            public boolean filter(Long num) throws Exception {
                return (num > 0);
            }
        });

        iteration.closeWith(stillGreaterThanZero);

        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {

            @Override
            public boolean filter(Long num) throws Exception {
                return (num < 0);
            }
        });

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
