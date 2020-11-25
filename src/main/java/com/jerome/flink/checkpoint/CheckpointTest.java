package com.jerome.flink.checkpoint;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * This is Description
 *
 * @author Jerome丶子木
 * @date 2020/01/13
 */
public class CheckpointTest {

    public static void main(String[] args) throws Exception {

        String hostname;
        int port;

        try{
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            hostname = parameterTool.get("hostname");
            port = parameterTool.getInt("port");
        }catch (Exception e){
            e.printStackTrace();
            hostname = "localhost";
            port = 9010;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置每隔5000ms启动一个checkpoint
        env.enableCheckpointing(1000);
        // 设置statebackend，即checkpoint存储位置
        env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/jerome/flink/checkpoint"));
        // 获取checkpoint配置参数
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 确保checkpoint之间至少有500ms的时间间隔，即checkpoint的最小间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 设置checkpoint超时时间，超过时间则会被丢弃
        checkpointConfig.setCheckpointTimeout(60000);
        // 同一时间只允许进行一个checkpoint
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 一旦Flink程序被cancel后，会保留checkpoint数据，
        // ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        // ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        String delimiter = "\n";

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream(hostname, port, delimiter);

        SingleOutputStreamOperator<WordWithCount> windowCount = stringDataStreamSource.flatMap(new RichFlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String word : splits) {
                    out.collect(new WordWithCount(word, 1));
                }
            }
        }).keyBy("word")
                //指定时间窗口时间是2秒，指定时间间隔是1秒
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum("count");
                /*.reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                        return new WordWithCount(value1.word,value1.count + value2.count);
                    }
                })*/

        // 把数据打印到控制台并且设置并行度
        windowCount.print().setParallelism(1);

        env.execute("Socket window count");


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
