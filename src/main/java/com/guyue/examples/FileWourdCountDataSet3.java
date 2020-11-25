package com.guyue.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

/**
 * Created by lipeng
 * com.guyue.flink
 * lipeng
 * 2019/3/12
 */
public class FileWourdCountDataSet3 {
    public static String path       = "/Users/lipeng/Downloads/";
    public static String inputName  = "tez.txt";
    public static String outputName = inputName + ".out";

    public static void main(String[] args) {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> textDS = environment.readTextFile(path + inputName);
        DataSet<Tuple2<String, Integer>> wordFreqDS = textDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value,
                                Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = value.toLowerCase().split("\\W+");
                for (String s : tokens) {
                    if (s.length() > 0) {
                        collector.collect(new Tuple2<String, Integer>(s.trim(), 1));
                    }
                }
            }
        }).groupBy(0).sum(1);

        // 2019-03-12 step-1
        // wordFreqDS.writeAsCsv(output);


        // 2019-03-12 step-3
        TextOutputFormat<Tuple2<String, Integer>> outputFormat = new TextOutputFormat<Tuple2<String, Integer>>(new Path(path));
        wordFreqDS.write(outputFormat, path + outputName);

        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
