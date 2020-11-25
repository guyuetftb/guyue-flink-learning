package com.jerome.flink.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * This is Description
 *
 * @author Jerome丶子木
 * @date 2019/12/19
 */
public class KafkaSourceTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();

        props.setProperty("bootstrap.servers","localhost:9092");
        props.setProperty("group.id","test123");
        props.setProperty("auto.offset.reset","earliest");
//        props.setProperty("enable.auto.commit","false");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), props);


        DataStreamSource<String> lines = env.addSource(kafkaSource);

        lines.print();

        env.execute("KafkaSourceSimpleTest");
    }

}
