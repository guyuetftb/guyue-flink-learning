package com.jerome.utils;


import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This is Description
 *
 * @author Jerome丶子木
 * @date 2020/01/02
 */
public class KafkaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtil.class);

    private ZkUtils zkUtils;

    public KafkaUtil(String zkHostPort,
                     int sessionTimeout,
                     int connectionTimeout) {
        this.zkUtils = ZkUtils.apply(zkHostPort, sessionTimeout, connectionTimeout, JaasUtils.isZkSecurityEnabled());
    }

    /**
     * 获取所有topic
     *
     * @return topic集合
     */
    public List getAllTopics() {
        Seq<String> allTopics = zkUtils.getAllTopics();
        return JavaConversions.seqAsJavaList(allTopics);
    }

    /**
     * 创建topic
     *
     * @param topicName
     * @param numPartition
     * @param replicationFactor
     */
    public boolean createTopic(String topicName,
                               int numPartition,
                               int replicationFactor) {
        if (numPartition > 10 || replicationFactor > 3) {
            LOG.error("创建topic失败，分区数不能大于10 并且副本数不能大于3");
            return false;
        } else {
            AdminUtils.createTopic(zkUtils, topicName, numPartition, replicationFactor, new Properties(), RackAwareMode.Enforced$.MODULE$);
            return true;
        }
    }

    /**
     * 判断topic是否存在
     *
     * @param topicName
     * @return
     */
    public boolean exists(String topicName) {
        return getAllTopics().contains(topicName);
    }

    /**
     * 修改topic属性
     */
    public void update() {
        ZkUtils zkUtils = ZkUtils.apply("localhost:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "test");
        // 增加topic级别属性
        props.put("min.cleanable.dirty.ratio", "0.3");
        // 删除topic级别属性
        props.remove("max.message.bytes");
        // 修改topic 'test'的属性
        AdminUtils.changeTopicConfig(zkUtils, "test", props);
        zkUtils.close();

    }

    public void select() {
        ZkUtils zkUtils = ZkUtils.apply("localhost:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "blg_gms_order_etl");
        System.out.println(props.toString());
        for (Object o : props.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            Object key = entry.getKey();
            Object value = entry.getValue();
            System.out.println("#########" + key + " = " + value);
        }
        zkUtils.close();
    }

    /**
     * 返回topic的分区数
     *
     * @param topic
     * @return
     */
    public int getPartition(String topic) {
        Process process = null;
        int count = -1;
        try {
            process = Runtime.getRuntime().exec("desc.sh " + topic);
            BufferedReader bufrIn = new BufferedReader(new InputStreamReader(process.getInputStream()));
            while ((bufrIn.readLine()) != null) {
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }

    /**
     * 获取partition数量
     *
     * @param topic
     * @return
     */
    public static int getPartitions(ZkClient zkClient,
                                    String topic) {
        return zkClient.getChildren("/brokers/topics/" + topic + "/partitions").size();
    }

    /**
     * 获取所有topic
     *
     * @return topic集合
     */
    public static List getAllTopics(ZkClient zkClient) {
        return zkClient.getChildren("/brokers/topics");
    }

    /**
     * 创建topic
     *
     * @param topicName
     * @param numPartition
     * @param replicationFactor
     */
    public static boolean createTopic(ZkUtils zkUtils,
                                      String topicName,
                                      int numPartition,
                                      int replicationFactor) {
        if (numPartition > 10 || replicationFactor > 3) {
            LOG.error("创建topic失败，分区数不能大于10 并且副本数不能大于3");
            return false;
        } else {
            AdminUtils.createTopic(zkUtils, topicName, numPartition, replicationFactor, new Properties(), RackAwareMode.Enforced$.MODULE$);
            return true;
        }
    }

    /**
     * 判断topic是否存在
     *
     * @param topicName
     * @return
     */
    public static boolean exists(ZkClient zkClient,
                                 String topicName) {
        return getAllTopics(zkClient).contains(topicName);
    }

    public void close() {
        if (zkUtils != null) {
            LOG.warn("关闭zkutil");
            zkUtils.close();
        }
    }

    public static Properties getConsumerPro(ParameterTool parameterTool) {
        Properties consumerPro = new Properties();
        consumerPro.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parameterTool.get("bs", "10.2.40.10:9092"));
        consumerPro.setProperty(ConsumerConfig.GROUP_ID_CONFIG, parameterTool.get("gid", "test"));
        consumerPro.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, parameterTool.get("reset", "latest"));
        consumerPro.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerPro.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "60000");
        return consumerPro;
    }

    public static Properties getProducerPro(ParameterTool parameterTool) {
        /* init */
        Properties producerPro = new Properties();
        producerPro.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parameterTool.get("bs", "10.2.40.10:9092"));
        /* default configuration */
        producerPro.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerPro.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerPro.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        producerPro.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        producerPro.put(ProducerConfig.RETRIES_CONFIG, 100);
        return producerPro;
    }

    public static void main(String[] args) {
    }


}
