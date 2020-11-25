package com.jerome.utils.impl;

import com.jerome.utils.KafkaProducer230;
import com.jerome.utils.KafkaProducer230.KafkaProducer230Factory;
import com.jerome.utils.enums.KafkaClusterType;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;


/**
 * This is Description
 *
 * @author KONG BIN
 * @date 2019/12/12
 */
public class KafkaProducerTest {

    public static void main(String[] args) {

        String[] content = new String[10];
        long timeMillis = System.currentTimeMillis();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date = dateFormat.format(timeMillis);

        content[0] = String.format("{\"name\":\"caocao\",\"age\":18,\"sex\":\"male\",\"date\":\"%s\"}",date);
        content[1] = String.format("{\"name\":\"guangyu\",\"age\":18,\"sex\":\"male\",\"date\":\"%s\"}",date);
        content[2] = String.format("{\"name\":\"zhangfei\",\"age\":18,\"sex\":\"male\",\"date\":\"%s\"}",date);
        content[3] = String.format("{\"name\":\"liubei\",\"age\":18,\"sex\":\"male\",\"date\":\"%s\"}",date);
        content[4] = String.format("{\"name\":\"zhaoyun\",\"age\":18,\"sex\":\"male\",\"date\":\"%s\"}",date);
        content[5] = String.format("{\"name\":\"xiaoqiao\",\"age\":18,\"sex\":\"male\",\"date\":\"%s\"}",date);
        content[6] = String.format("{\"name\":\"daqiao\",\"age\":18,\"sex\":\"male\",\"date\":\"%s\"}",date);
        content[7] = String.format("{\"name\":\"zhugeliang\",\"age\":18,\"sex\":\"male\",\"date\":\"%s\"}",date);
        content[8] = String.format("{\"name\":\"zhouyu\",\"age\":18,\"sex\":\"male\",\"date\":\"%s\"}",date);
        content[9] = String.format("{\"name\":\"huanggai\",\"age\":18,\"sex\":\"male\",\"date\":\"%s\"}",date);

        String topic = "people";
        KafkaProducer230 kafkaProducer = (KafkaProducer230) KafkaProducer230Factory.createKafkaProducer();

        for (int index = 0 ; index < content.length ; index ++ ){
            kafkaProducer.send(new ProducerRecord(topic,null,content[index]));
        }
        kafkaProducer.close();

    }
}
