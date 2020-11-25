package com.jerome.flink.transformation;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;

/**
 * This is Description
 *
 * @author KONG BIN
 * @date 2019/12/12
 */
public class IteratTest {

    public static void main(String[] args) throws Exception {

        ParameterTool parameter = ParameterTool.fromArgs(args);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","localhost:9092");


        String topic = parameter.get("topic","test");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), prop);

        kafkaConsumer.setStartFromEarliest();

        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        IterativeStream<People> iterate = source.map(new MapFunction<String, People>() {
            @Override
            public People map(String value) throws Exception {
                System.out.println(value);
                return JSONObject.parseObject(value, People.class);
            }
        }).iterate();

        SingleOutputStreamOperator<People> feedback = iterate.filter(new FilterFunction<People>() {
            @Override
            public boolean filter(People people) throws Exception {
                return "caocao".equals(people.name);
            }
        });

        //如果有复合的feedback过滤条件的数据，比如name为caocao的，会不断的循环输出
        feedback.print("feedback: ");

        iterate.closeWith(feedback);

        SingleOutputStreamOperator<People> result = iterate.filter(new FilterFunction<People>() {
            @Override
            public boolean filter(People people) throws Exception {
                return !"caocao".equals(people.name);
            }
        });

        result.print("result: ");

        SplitStream<People> split = iterate.split(new OutputSelector<People>() {
            @Override
            public Iterable<String> select(People people) {
                ArrayList<String> list = new ArrayList<>();
                if ("male".equals(people.sex)) {
                    list.add("male");
                } else {
                    list.add("female");
                }

                return list;
            }
        });

        DataStream<People> male = split.select("male");

        male.print("male: ");

        iterate.closeWith(male);

        env.execute("IterateOperator");


    }

    static class People {
        public String name;
        public int age;
        public String sex;
        public String date;

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        public People() {
        }

        public People(String name, int age, String sex) {
            this.name = name;
            this.age = age;
            this.sex = sex;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public String getSex() {
            return sex;
        }

        public void setSex(String sex) {
            this.sex = sex;
        }

        public People(String name, int age, String sex, String date) {
            this.name = name;
            this.age = age;
            this.sex = sex;
            this.date = date;
        }

        @Override
        public String toString() {
            return "People{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", sex='" + sex + '\'' +
                    ", date='" + date + '\'' +
                    '}';
        }
    }

}
