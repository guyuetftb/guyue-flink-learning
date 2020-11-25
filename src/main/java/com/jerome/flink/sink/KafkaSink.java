package com.jerome.flink.sink;

import com.jerome.utils.constant.Constant;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

/**
 * This is Description
 *
 * @author Jerome丶子木
 * @date 2019/12/23
 */
public class KafkaSink extends RichSinkFunction<String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);

    private String bootstrapServers;
    private String topic;
    private KafkaProducer<String,String> producer;

    public KafkaSink() {
    }

    public KafkaSink(String bootstrapServers, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Properties props = new Properties();
        props.setProperty(Constant.KAFKA_PRODUCER_BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.setProperty(Constant.KAFKA_PRODUCER_KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(Constant.KAFKA_PRODUCER_VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(Constant.KAFKA_PRODUCER_COMPRESSION_TYPE_CONFIG, "lz4");
        props.setProperty(Constant.KAFKA_PRODUCER_ACKS_CONFIG, "all");
        props.setProperty(Constant.KAFKA_PRODUCER_RETRIES_CONFIG, "50");
        producer = new KafkaProducer<>(props);
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        producer.close();
        super.close();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        producer.send(new ProducerRecord<>(this.topic, UUID.randomUUID().toString(),value));
    }
}
