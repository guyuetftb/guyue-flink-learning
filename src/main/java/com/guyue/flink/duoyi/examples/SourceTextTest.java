package com.guyue.flink.duoyi.examples;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName SourceParallelTest
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-02 22:40
 */
public class SourceTextTest {

    public static void main(String[] args) throws Exception {
        String basePath = System.getProperty("user.dir");
        System.out.println(basePath);
        args = new String[]{basePath + "/guyue-flink-learning/src/main/data/test/words.txt"};

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> words = env.readTextFile(args[0]);

        // 默认情况下, 并行度等于当前机器可用的 "逻辑核数", 逻辑核数不是物理核数, 2核4线程, 逻辑核等于4.
        int defaultParallelism = words.getParallelism();
        System.out.println("++++++++++++++++++> default parallelism = " + defaultParallelism);

        DataStream<Tuple2<String, Integer>> everyWords = words.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split("\\s+");
                for (String word : words) {
                    if (StringUtils.isEmpty(word)) {
                        continue;
                    }
                    collector.collect(Tuple2.of(word.toLowerCase(), 1));
                }
            }
        }).setParallelism(3);

        int realParallelism = everyWords.getParallelism();

        System.out.println("==================> real parallelism = " + realParallelism);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = everyWords.keyBy(0).sum(1);
        sum.print("--->");

        env.execute("SourceTextTest");

    }
}
