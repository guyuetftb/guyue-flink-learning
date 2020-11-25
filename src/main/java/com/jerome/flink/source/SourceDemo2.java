package com.jerome.flink.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

/**
 * This is Description
 *
 * @author Jerome丶子木
 * @date 2019/12/19
 */
public class SourceDemo2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<Long> nums = env.fromParallelCollection(new NumberSequenceIterator(1, 20), TypeInformation.of(Long.TYPE));

//        DataStreamSource<Long> nums = env.fromParallelCollection(new NumberSequenceIterator(2, 20), Long.class);

        DataStreamSource<Long> nums = env.generateSequence(1, 100);

//        DataStreamSource<String> stringDataStreamSource = env.readTextFile("file:///path/to/file");

//        DataStreamSource<String> stringDataStreamSource = env.readFile(new CsvInputFormat<String>(new Path("file:///path/to/file")) {
//            @Override
//            protected String fillRecord(String reuse, Object[] parsedValues) {
//                return null;
//            }
//        }, "file:///path/to/file");

        int parallelism = nums.getParallelism();

        System.out.println("source parallelism is =============> " + parallelism);

        SingleOutputStreamOperator<Long> filter = nums.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long integer) throws Exception {
                return integer % 2 == 0;
            }
        });

        int parallelism1 = filter.getParallelism();

        System.out.println("transformation parallelism is =============> " + parallelism1);

        filter.print();

        env.execute("SourceDemo2");
    }
}
