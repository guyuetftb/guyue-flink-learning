package com.zk.utils;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author zhangkai
 * @create 2020/1/15
 */
public class ConsumerRecordKafkaDeserializationSchema implements KafkaDeserializationSchema<ConsumerRecord> {
    @Override
    public boolean isEndOfStream(ConsumerRecord nextElement) {
        return false;
    }

    @Override
    public ConsumerRecord deserialize(ConsumerRecord<byte[], byte[]> record) {
        return record;
    }

    @Override
    public TypeInformation<ConsumerRecord> getProducedType() {
        return TypeInformation.of(ConsumerRecord.class);
    }
}