package com.jerome.flink.etl;

import com.alibaba.fastjson.JSONObject;
import com.jerome.flink.sink.KafkaSink;
import com.jerome.utils.KafkaProducer230;
import com.jerome.utils.constant.Constant;
import com.jerome.utils.enums.SeparatorEnum;
import org.I0Itec.zkclient.ZkClient;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * This is Description
 *
 * @author Jerome丶子木
 * @date 2019/12/19
 */
public class TextLogEtl {
    private static final Logger logger = LoggerFactory.getLogger(TextLogEtl.class);

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        //判断执行环境
        String osName = System.getProperty("os.name");
        String bootstrapServers;
        String sinkBootstrapServers;
        String zookeeperServers;
        if ("Linux".equals(osName)) {
            bootstrapServers = params.get("bootstrapServers", "10.3.32.20:9092");
            sinkBootstrapServers = params.get("sinkBootstrapServers", "10.3.32.20:9092,10.3.32.22:9092");
            zookeeperServers = params.get("zookeeperServers","10.3.32.20:2181");
        } else {
            bootstrapServers = params.get("bootstrapServers", "localhost:9092");
            sinkBootstrapServers = params.get("sinkBootstrapServers", "localhost:9092");
            zookeeperServers = params.get("zookeeperServers","localhost:2181");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String groupId = params.get("groupId", "test");
        //earliest
        String offsetReset = params.get("offsetReset", "latest");
        String topic = params.get("sourceTopic");
        String sinkTopic = params.get("sinkTopic");
        String startFromTimestamp = params.get("startTimestamp");

        Properties kafkaProp = new Properties();
        kafkaProp.setProperty(Constant.KAFKA_CONSUMER_BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        kafkaProp.setProperty(Constant.KAFKA_CONSUMER_GROUP_ID_CONFIG,groupId);
        kafkaProp.setProperty(Constant.KAFKA_CONSUMER_AUTO_OFFSET_RESET_CONFIG,offsetReset);
        //动态感知kafka主题分区的增加  单位毫秒
        kafkaProp.setProperty("flink.partition-discovery.interval-millis", "5000");
        System.out.println("===============>  任务开始 =================》");

//        DataStreamSource<String> simpleKafkaSource = KafkaSource.getSimpleKafkaSource(env, Collections.singletonList(topic), kafkaProp, startFromTimestamp);

        FlinkKafkaConsumer<String> simpleKafkaSource = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), kafkaProp);
        DataStreamSource<String> stringDataStreamSource = env.addSource(simpleKafkaSource);
        //获取kafka分区
//        int partitions = KafkaUtil.getPartitions(new ZkClient(zookeeperServers, 5000), topic);

        int partitions = new ZkClient(zookeeperServers, 5000).getChildren("/brokers/topics/" + topic + "/partitions").size();

        //设置并行度
        DataStreamSource<String> kafkaDataStreamSource = stringDataStreamSource.setParallelism(partitions);

        //flume 的分隔头
        String HEAD_AND_BODY_SPLIT = SeparatorEnum.getSeparatorEnum("FLUME_HEAD_AND_BODY_SPLIT").separator;
        String HEAD_SPLIT = SeparatorEnum.getSeparatorEnum("FLUME_HEAD_SPLIT").separator;
        String separatorName = params.get("separator");
        String separator = SeparatorEnum.getSeparatorEnum(separatorName).separator;

        //解析逻辑
        SingleOutputStreamOperator<String> jsonObjectMaped = kafkaDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String sourceLine) throws Exception {

                JSONObject resultJsonObj = new JSONObject();

                String[] headAndBody = sourceLine.split(HEAD_AND_BODY_SPLIT);
                String bodyLine;

                // 判断是否存在头
                if (headAndBody.length == 2) {
                    String[] headSplits = headAndBody[0].split(HEAD_SPLIT);
                    for (String head : headSplits) {
                        String[] keyAndValue = head.split(":");
                        resultJsonObj.put(keyAndValue[0], keyAndValue[1]);
                    }
                    bodyLine = headAndBody[1];
                } else {
                    bodyLine = sourceLine;
                }

                String[] splitLine = bodyLine.split(separator);

                for (int i = 0; i < splitLine.length; i++) {
                    resultJsonObj.put("field" + i, splitLine[i]);
                }

                return resultJsonObj.toJSONString();
            }
        });


//        jsonObjectMaped.map(new MapFunction<JSONObject, String>() {
//            @Override
//            public String map(JSONObject jsonObject) throws Exception {
//                return jsonObject.toString();
//            }
//        }).print();

        //写入目标topic
        jsonObjectMaped.addSink(new KafkaSink(sinkBootstrapServers,sinkTopic));


        env.execute("TextLogEtl");


    }
}
