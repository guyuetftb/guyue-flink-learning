package com.jerome.flink.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * This is Description
 *
 * @author Jerome丶子木
 * @date 2019/12/19
 */
public class SourceDemo1 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5, 6, 7);

//        DataStreamSource<String> nums = env.socketTextStream("localhost", 12800);

        DataStreamSource<Integer> nums = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7));

        int parallelism = nums.getParallelism();

        System.out.println("source parallelism is =============> " + parallelism);

        SingleOutputStreamOperator<Integer> filter = nums.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer % 2 == 0;
            }
        });

        int parallelism1 = filter.getParallelism();

        System.out.println("transformation parallelism is =============> " + parallelism1);

        filter.print();

        env.execute("SourceDemo1");


    }

}
