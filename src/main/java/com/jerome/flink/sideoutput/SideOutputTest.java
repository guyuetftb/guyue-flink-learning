package com.jerome.flink.sideoutput;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * This is Description
 *
 * @author Jerome丶子木
 * @date 2020/01/13
 */
public class SideOutputTest {

    public static void main(String[] args) throws Exception {

        String host;
        int port;

        try{
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            host = parameterTool.get("hostname");
            port = parameterTool.getInt("port");
        }catch (Exception e){
            e.printStackTrace();
            host = "localhost";
            port = 9010;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String delimiter = "\n";

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream(host, port, delimiter);

        OutputTag<String> stringOutputTag = new OutputTag<String>("gt5word"){};

        SingleOutputStreamOperator<Tuple2<String, Integer>> processedStream = stringDataStreamSource.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                String[] splits = value.split("\\W+");

                for (String word : splits) {
                    if (word.length() > 5) {
                        ctx.output(stringOutputTag, word);
                    } else {
                        out.collect(new Tuple2<>(word, 1));
                    }
                }

            }
        });

        SingleOutputStreamOperator<String> sideOutputStream = processedStream.getSideOutput(stringOutputTag).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "side out put stream is :" + value;
            }
        });

        OutputTag<String> gt6word = new OutputTag<String>("gt6word"){};

        SingleOutputStreamOperator<String> process6 = sideOutputStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

                String[] split = value.split(":");
                if (split[1].length() > 10) {
                    ctx.output(gt6word, split[1]);
                } else {
                    out.collect(split[1]);
                }

            }
        });

        DataStream<String> sideOutput = process6.getSideOutput(gt6word).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "gt6word is :" + value;
            }
        });

        sideOutput.print();

        process6.print();

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = processedStream.keyBy(0).sum(1);

        sum.print();

        sideOutputStream.print();

        env.execute("side out put test ");


    }
}
