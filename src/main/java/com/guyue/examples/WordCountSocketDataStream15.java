package com.guyue.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Created by lipeng
 * com.guyue.flink
 * lipeng
 * 2019/3/12
 */
public class WordCountSocketDataStream15 {
    public static void main(String[] args) throws Exception {

        // the port to connect to
        final int port;
        final String host;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            host = params.get("host");
            port = params.getInt("port");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'WordCountSocketDataSet --host <ip> --port <port>'");
            return;
        }

        System.out.print("------------------------------------------------------");
        System.out.print("------------ host = " + host + ", port = " + port + "-------------------------");
        System.out.print("------------------------------------------------------");
        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println(" env ");
        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(host, port, "\n");
        System.out.println(" env ");

        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts = text
                .flatMap(new FlatMapFunction<String, WordWithCount>() {

                    @Override
                    public void flatMap(String value,
                                        Collector<WordWithCount> out) {
                        for (String word : value.split("\\s")) {
                            out.collect(new WordWithCount(word, 1L));
                        }
                    }
                })
                .keyBy("word")
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .reduce(new ReduceFunction<WordWithCount>() {

                    @Override
                    public WordWithCount reduce(WordWithCount a,
                                                WordWithCount b) {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }

    // Data type for words with count
    public static class WordWithCount {

        public String word;
        public long   count;

        public WordWithCount() {
        }

        public WordWithCount(String word,
                             long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
