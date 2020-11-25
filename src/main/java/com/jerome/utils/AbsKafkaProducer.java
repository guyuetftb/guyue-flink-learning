package com.jerome.utils;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.concurrent.Future;

/**
 * This is Description
 *
 * @author KONG BIN
 * @date 2019/12/12
 */
public abstract class AbsKafkaProducer<K, V> {

    protected KafkaProducer producer = null;

    public AbsKafkaProducer(KafkaProducer producer){
        this.producer = producer;
    }

    /**
     * abstract method send
     */
    public abstract Future<RecordMetadata> send(ProducerRecord<K, V> record);

    /**
     * abstract method send
     */
    public abstract Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);

    /**
     * abstract method abortTransaction
     */
    public abstract void abortTransaction();

    /**
     * abstract method beginTransaction
     */
    public abstract void beginTransaction();

    /**
     * abstract method commitTransaction
     */
    public abstract void commitTransaction();

    /**
     * abstract method initTransactions
     */
    public abstract void initTransactions();

    /**
     * abstract method partitionsFor
     */
    public abstract List<PartitionInfo> partitionsFor(final String topic);

    /**
     * abstract method send
     */
    public abstract void close();

    /**
     * abstract method send
     */
    public abstract void flush();

    /**
     * abstract method version
     */
    public abstract String version();

}
