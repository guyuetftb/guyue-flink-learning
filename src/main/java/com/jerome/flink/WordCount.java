package com.jerome.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * This is Description
 *
 * @author KONG BIN
 * @date 2019/12/09
 */
public class WordCount {
    public static void main(String[] args) throws Exception{

        if (args.length != 2){
            System.err.println("Usage:\nSocketTextStreamWordCount <hostname> <port>");
            return;
        }

        String hostname = args[0];
        Integer port = Integer.parseInt(args[1]);

        //设定执行环境设定
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 指定数据源地址，读取输入数据
//        DataStreamSource<String> stringDataStreamSource = env.readTextFile("/Users/kongbin/develop/test/text/text1.txt");

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream(hostname, port);

        //对数据集执行转换操作逻辑
        DataStream<WordWithCount> windowCount = stringDataStreamSource.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<WordWithCount> collector) throws Exception {
                String regex = "\\s";
                String[] splits = value.toLowerCase().split(regex);
                for (String word : splits) {
                    System.out.println(word);
                    collector.collect(new WordWithCount(word, 1L));
                }
            }
        })//打平操作，把每行单词转化为<word，count>类型的数据
                .keyBy("word") //针对相同的word数据进行分组
                .timeWindow(Time.seconds(2),Time.seconds(1))//指定计算数据的窗口大小和滑动窗口大小
                .sum("count");

        //把数据打印到控制台
        windowCount.print()
                .setParallelism(1); //设置并行度1

        //flink是懒加载，所以必须使用execute方法，上面才会执行
        env.execute("WordCount");
    }


    public static class WordWithCount{
        public String word;
        public long count;
        public WordWithCount(){}

        public WordWithCount(String word, long count){
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString(){
            StringBuilder res = new StringBuilder();
            res.append("WordWithCount{ word = '");
            res.append(word);
            res.append("' , count= ");
            res.append(count);
            res.append("} ;");
            return res.toString();
        }
    }

}
