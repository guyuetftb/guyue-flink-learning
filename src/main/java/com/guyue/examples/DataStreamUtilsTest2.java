package com.guyue.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by lipeng
 * com.guyue.flink
 * lipeng
 * 2019/3/18
 */
public class DataStreamUtilsTest2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> dataStream = env.fromElements(1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l);

        DataStream<Long> greaterThanFive = dataStream.map(new MapFunction<Long, Long>() {

            @Override
            public Long map(Long num) throws Exception {
                System.out.println("=============== num = " + num);
                return num - 2;
            }
        });

        try {
            Iterator<Long> iterator = DataStreamUtils.collect(greaterThanFive);
            for (;iterator.hasNext();) {
                Long s = iterator.next();
                System.out.println("-------" + s);
            }
            env.execute();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
