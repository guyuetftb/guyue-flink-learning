package com.jerome.utils;

import com.jerome.utils.constant.Constant;

import java.util.Properties;

/**
 * This is Description
 *
 * @author KONG BIN
 * @date 2019/12/12
 */
public class AbsKafkaProducerFactory {
    /**
     * <p> props.put("acks", "all");
     * <p> props.put("retries", 30);
     * <p> props.put("compression.type", "lz4");
     * <p> props.put("batch.size", 307200);
     * <p> props.put("linger.ms", 30);
     * <p> props.put("max.in.flight.requests.per.connection","1");
     * <p> props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
     * <p> props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
     */
    protected static Properties getProducerDefaultConfiguration() {
        Properties properties = new Properties();
        properties.setProperty(Constant.KAFKA_PRODUCER_ACKS_CONFIG, "all");
        properties.setProperty(Constant.KAFKA_PRODUCER_RETRIES_CONFIG, "50");
        properties.setProperty(Constant.KAFKA_PRODUCER_COMPRESSION_TYPE_CONFIG, "lz4");
        properties.setProperty(Constant.KAFKA_PRODUCER_BATCH_SIZE_CONFIG, "307200");
        properties.setProperty(Constant.KAFKA_PRODUCER_LINGER_MS_CONFIG, "30");
        properties.setProperty(Constant.KAFKA_PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        properties.setProperty(Constant.KAFKA_PRODUCER_KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(Constant.KAFKA_PRODUCER_VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

}
