package com.jerome.flink.etl.templatemethod;

import com.jerome.utils.constant.Constant;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.util.Properties;

/**
 * This is Description
 *
 * @author Jerome丶子木
 * @date 2020/01/02
 */
public class AbstractEtl {

    private StreamExecutionEnvironment env;
    private ParameterTool parameterTool;
    private SingleOutputStreamOperator<String> stream;

    private void createFlinkEnv(){
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,200));
        String checkpointPath = parameterTool.get("checkpoint_path");

        if (!"".equals(checkpointPath) && checkpointPath != null){
            CheckpointConfig checkpointConfig = env.getCheckpointConfig();
            env.enableCheckpointing(20000);
            checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
            checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            StateBackend rocksDBStateBackend = null;
            try {
                rocksDBStateBackend = new RocksDBStateBackend(checkpointPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
            env.setStateBackend(rocksDBStateBackend);
        }
    }

    private void sourceKafka(){
        System.out.println("add source kafka ");
        //判断执行环境
        String osName = System.getProperty("os.name");
        String bootstrapServers;
        String sinkBootstrapServers;
        String zookeeperServers;
        if ("Linux".equals(osName)) {
            bootstrapServers = parameterTool.get("bootstrapServers", "10.3.32.20:9092");
            sinkBootstrapServers = parameterTool.get("sinkBootstrapServers", "10.3.32.20:9092,10.3.32.22:9092");
            zookeeperServers = parameterTool.get("zookeeperServers","10.3.32.20:2181");
        } else {
            bootstrapServers = parameterTool.get("bootstrapServers", "localhost:9092");
            sinkBootstrapServers = parameterTool.get("sinkBootstrapServers", "localhost:9092");
            zookeeperServers = parameterTool.get("zookeeperServers","localhost:2181");
        }
        String groupId = parameterTool.get("groupId", "test");
        //earliest
        String offsetReset = parameterTool.get("offsetReset", "latest");
        String topic = parameterTool.get("sourceTopic");
        String sinkTopic = parameterTool.get("sinkTopic");
        String startFromTimestamp = parameterTool.get("startTimestamp");

        Properties kafkaProp = new Properties();
        kafkaProp.setProperty(Constant.KAFKA_CONSUMER_BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        kafkaProp.setProperty(Constant.KAFKA_CONSUMER_GROUP_ID_CONFIG,groupId);
        kafkaProp.setProperty(Constant.KAFKA_CONSUMER_AUTO_OFFSET_RESET_CONFIG,offsetReset);
        //动态感知kafka主题分区的增加  单位毫秒
        kafkaProp.setProperty("flink.partition-discovery.interval-millis", "5000");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(parameterTool.get("topics", "test"), new SimpleStringSchema(), kafkaProp);

        stream = env.addSource(kafkaConsumer);

    }



}
