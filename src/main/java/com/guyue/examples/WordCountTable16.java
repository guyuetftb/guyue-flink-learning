package com.guyue.examples;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * Created by lipeng
 * com.guyue.flink
 * lipeng
 * 2019/3/13
 */
public class WordCountTable16 {
    public static void main(String[] args) {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createCollectionsEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(executionEnvironment);

//        DataSet<WC> input = executionEnvironment.fromElements(new WC("hello",1), new WC("hello",2),new WC("world",2), new WC("word",1));
//
//        TableSource tableSource = null;
//
//        tEnv.registerDataSetInternal("WordCount",input);
//
//        DataSet<WC> dataSet = executionEnvironment.fromElements(new WC("Hello", 1), new WC("hello", 1), new WC("world", 1));
//        Expression[] expressions = new Expression[2];
//        tEnv.registerDataSetInternal("WordCount", dataSet);
//        Table table = tEnv.sqlQuery("select word, SUM(frequency) as frequency from WordCount group by word");
//        // result.print();
//
//        try {
//            executionEnvironment.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    public static class WC {
        public String word;
        public long frequency;

        // public constructor to make it a Flink POJO
        public WC() {}

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "WC " + word + " " + frequency;
        }
    }
}
