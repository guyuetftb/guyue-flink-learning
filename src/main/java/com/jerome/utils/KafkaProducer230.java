package com.jerome.utils;

import com.jerome.utils.constant.Constant;
import com.jerome.utils.enums.KafkaClusterType;
import com.jerome.utils.enums.KafkaVersionType;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * This is Description
 *
 * @author KONG BIN
 * @date 2019/12/12
 */
public class KafkaProducer230 extends AbsKafkaProducer {


    private KafkaProducer230(KafkaProducer producer){
        super(producer);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord record) {
        return this.producer.send(record);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord record, Callback callback) {
        return this.producer.send(record,callback);
    }

    @Override
    public void abortTransaction() {

    }

    @Override
    public void beginTransaction() {

    }

    @Override
    public void commitTransaction() {

    }

    @Override
    public void initTransactions() {

    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return this.producer.partitionsFor(topic);
    }

    @Override
    public void close() {
        if (null != this.producer){
            this.producer.close();
        }
    }

    public void close(Duration timeout){
        if (null != this.producer){
            this.producer.close(timeout);
        }
    }

    @Override
    public void flush() {
        if (null != this.producer){
            this.producer.flush();
        }

    }

    @Override
    public String version() {
        return KafkaVersionType.V_230.name();
    }

    public static class KafkaProducer230Factory extends AbsKafkaProducerFactory{

        public static AbsKafkaProducer createKafkaProducer(KafkaClusterType cluster){
            return createKafkaProducerInternal(cluster,null);
        }

        public static AbsKafkaProducer createKafkaProducer(){
            return createKafkaProducerInternal(KafkaClusterType.LOCAL,null);

        }

        public static AbsKafkaProducer createKafkaProducer(KafkaClusterType cluster, Properties properties){
            return createKafkaProducerInternal(cluster,properties);
        }


        private static AbsKafkaProducer createKafkaProducerInternal(KafkaClusterType cluster, Properties properties){
            Properties propertiesInternal = getProducerDefaultConfiguration();
            if (null != properties){
                for (Map.Entry entry: properties.entrySet()){
                    propertiesInternal.put(entry.getKey().toString(),entry.getValue().toString());
                }
            }

            propertiesInternal.setProperty(Constant.KAFKA_PRODUCER_BOOTSTRAP_SERVERS_CONFIG,Constant.getKafkaServer(cluster));
            KafkaProducer producer = new KafkaProducer(propertiesInternal);
            KafkaProducer230 kafkaProducer230 = new KafkaProducer230(producer);
            return kafkaProducer230;
        }
    }

}
