package com.jerome.utils.constant;

import com.jerome.utils.enums.KafkaClusterType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;

/**
 * This is Description
 *
 * @author KONG BIN
 * @date 2019/12/12
 */
public class Constant {

    private static final Logger LOG = LoggerFactory.getLogger(Constant.class);

    private static final String CONFIG_PROPERTIES = "kafka.properties";

    /**
     * 离线
     */
    private static final String OFFLINE_KAFKA_SERVER;
    private static final String TEST_OFFLINE_KAFKA_SERVER;
    private static final String OFFLINE_ZOOKEEPER_SERVER;

    /**
     * 实时
     */
    private static final String REALTIME_KAFKA_SERVER;
    private static final String TEST_REALTIME_KAFKA_SERVER;
    private static final String REALTIME_ZOOKEEPER_SERVER;

    /**
     * 冷备
     */
    private static final String COLD_STANDBY_KAFKA_SERVER;
    private static final String TEST_COLD_STANDBY_KAFKA_SERVER;
    private static final String COLD_STANDBY_ZOOKEEPER_SERVER;

    /**
     * 热备
     */
    private static final String HOT_STANDBY_KAFKA_SERVER;
    private static final String TEST_HOT_STANDBY_KAFKA_SERVER;
    private static final String HOT_STANDBY_ZOOKEEPER_SERVER;

    /**
     * 本地
     */
    private static final String LOCAL_KAFKA_SERVER;
    private static final String LOCAL_ZOOKEEPER_SERVER;


    private static final HashMap<String,String> servers = new HashMap<String,String>();

    static {

        Properties props = new Properties();
        try {
            props.load(Constant.class.getClassLoader().getResourceAsStream(CONFIG_PROPERTIES));
        }catch (Exception e){
            e.printStackTrace();
            LOG.error("Error : Get Properties Failed. ", e);
            System.exit(-1);
        }

        //离线
        OFFLINE_KAFKA_SERVER = props.getProperty("offline.kafka.servers","");
        TEST_OFFLINE_KAFKA_SERVER = props.getProperty("test.offline.kafka.servers","");
        OFFLINE_ZOOKEEPER_SERVER = props.getProperty("offline.kafka.zookeepers","");

        //实时
        REALTIME_KAFKA_SERVER = props.getProperty("realtime.kafka.servers","");
        TEST_REALTIME_KAFKA_SERVER = props.getProperty("test.realtime.kafka.servers","");
        REALTIME_ZOOKEEPER_SERVER = props.getProperty("realtime.kafka.zookeepers","");

        //热备
        HOT_STANDBY_KAFKA_SERVER = props.getProperty("hot.standby.kafka.servers","");
        TEST_HOT_STANDBY_KAFKA_SERVER = props.getProperty("test.hot.standby.kafka.servers","");
        HOT_STANDBY_ZOOKEEPER_SERVER = props.getProperty("hot.standby.kafka.zookeepers","");

        //冷备
        COLD_STANDBY_KAFKA_SERVER = props.getProperty("cold.standby.kafka.servers","");
        TEST_COLD_STANDBY_KAFKA_SERVER = props.getProperty("test.cold.standby.kafka.servers","");
        COLD_STANDBY_ZOOKEEPER_SERVER = props.getProperty("cold.standby.kafka.zookeepers","");

        //local
        LOCAL_KAFKA_SERVER = props.getProperty("local.kafka.servers","");
        LOCAL_ZOOKEEPER_SERVER = props.getProperty("local.kafka.zookeepers","");

        servers.put(KafkaClusterType.OFFLINE.name(), OFFLINE_KAFKA_SERVER);
        servers.put(KafkaClusterType.OFFLINE_TEST.name(), TEST_OFFLINE_KAFKA_SERVER);

        servers.put(KafkaClusterType.REALTIME.name(), REALTIME_KAFKA_SERVER);
        servers.put(KafkaClusterType.REALTIME_TEST.name(), TEST_REALTIME_KAFKA_SERVER);

        servers.put(KafkaClusterType.HOT_STANDBY.name(), HOT_STANDBY_KAFKA_SERVER);
        servers.put(KafkaClusterType.HOT_STANDBY_TEST.name(), TEST_HOT_STANDBY_KAFKA_SERVER);

        servers.put(KafkaClusterType.COLD_STANDBY.name(), COLD_STANDBY_KAFKA_SERVER);
        servers.put(KafkaClusterType.COLD_STANDBY_TEST.name(), TEST_COLD_STANDBY_KAFKA_SERVER);

        servers.put(KafkaClusterType.LOCAL.name(),LOCAL_KAFKA_SERVER);

    }

    /**
     * Producer Configuration
     */
    public static final String KAFKA_PRODUCER_ACKS_CONFIG = ProducerConfig.ACKS_CONFIG;
    public static final String KAFKA_PRODUCER_RETRIES_CONFIG = ProducerConfig.RETRIES_CONFIG;
    public static final String KAFKA_PRODUCER_BATCH_SIZE_CONFIG = ProducerConfig.BATCH_SIZE_CONFIG;
    public static final String KAFKA_PRODUCER_BOOTSTRAP_SERVERS_CONFIG = ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
    public static final String KAFKA_PRODUCER_BUFFER_MEMORY_CONFIG = ProducerConfig.BUFFER_MEMORY_CONFIG;
    public static final String KAFKA_PRODUCER_COMPRESSION_TYPE_CONFIG = ProducerConfig.COMPRESSION_TYPE_CONFIG;
    public static final String KAFKA_PRODUCER_LINGER_MS_CONFIG = ProducerConfig.LINGER_MS_CONFIG;
    public static final String KAFKA_PRODUCER_PARTITIONER_CLASS_CONFIG = ProducerConfig.PARTITIONER_CLASS_CONFIG;
    public static final String KAFKA_PRODUCER_RECEIVE_BUFFER_CONFIG = ProducerConfig.RECEIVE_BUFFER_CONFIG;
    public static final String KAFKA_PRODUCER_SEND_BUFFER_CONFIG = ProducerConfig.SEND_BUFFER_CONFIG;
    public static final String KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS_CONFIG = ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
    public static final String KAFKA_PRODUCER_KEY_SERIALIZER_CLASS_CONFIG = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
    public static final String KAFKA_PRODUCER_MAX_BLOCK_MS_CONFIG = ProducerConfig.MAX_BLOCK_MS_CONFIG;
    public static final String KAFKA_PRODUCER_MAX_REQUEST_SIZE_CONFIG = ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
    public static final String KAFKA_PRODUCER_VALUE_SERIALIZER_CLASS_CONFIG = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
    public static final String KAFKA_PRODUCER_RETRY_BACKOFF_MS_CONFIG = ProducerConfig.RETRY_BACKOFF_MS_CONFIG;
    public static final String KAFKA_PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;



    /**
     * Consumer Configuration
     */
    public static final String KAFKA_CONSUMER_AUTO_COMMIT_INTERVAL_MS_CONFIG = ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
    public static final String KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG = ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
    public static final String KAFKA_CONSUMER_REQUEST_TIMEOUT_MS_CONFIG = ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG;
    public static final String KAFKA_CONSUMER_SESSION_TIMEOUT_MS_CONFIG = ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
    public static final String KAFKA_CONSUMER_ENABLE_AUTO_COMMIT_CONFIG = ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
    public static final String KAFKA_CONSUMER_BOOTSTRAP_SERVERS_CONFIG = ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
    public static final String KAFKA_CONSUMER_GROUP_ID_CONFIG = ConsumerConfig.GROUP_ID_CONFIG;
    public static final String KAFKA_CONSUMER_INTERCEPTOR_CLASSES_CONFIG = ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG;
    public static final String KAFKA_CONSUMER_SEND_BUFFER_CONFIG = ConsumerConfig.SEND_BUFFER_CONFIG;
    public static final String KAFKA_CONSUMER_FETCH_MAX_BYTES_CONFIG = ConsumerConfig.FETCH_MAX_BYTES_CONFIG;
    public static final String KAFKA_CONSUMER_FETCH_MIN_BYTES_CONFIG = ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
    public static final String KAFKA_CONSUMER_MAX_POLL_RECORDS_CONFIG = ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
    public static final String KAFKA_CONSUMER_KEY_DESERIALIZER_CLASS_CONFIG = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
    public static final String KAFKA_CONSUMER_VALUE_DESERIALIZER_CLASS_CONFIG = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
//    public static final String KAFKA_CONSUMER_GROUP_INSTANCE_ID_CONFIG = ConsumerConfig.GROUP_INSTANCE_ID_CONFIG;
    public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET_CONFIG = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

    public static String getKafkaServer(KafkaClusterType clusterType){
        return servers.get(clusterType.name());
    }

    public static void main(String[] args) {
        System.out.println(Constant.HOT_STANDBY_KAFKA_SERVER);
        System.out.println(Constant.COLD_STANDBY_KAFKA_SERVER);
        System.out.println(Constant.OFFLINE_KAFKA_SERVER);
        System.out.println(Constant.REALTIME_KAFKA_SERVER);
        System.out.println(Constant.LOCAL_KAFKA_SERVER);
    }


}
