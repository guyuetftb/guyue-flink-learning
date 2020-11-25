package com.guyue.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lipeng
 * com.guyue.flink
 * lipeng
 * 2019/4/18
 */
public class StreamingFlinkSQL2Join {

    public static Logger logger = LoggerFactory.getLogger(StreamingFlinkSQL2Join.class);

    private static String SQL = "select * from my_order";

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");

        // add source
        FlinkKafkaConsumer<String> orderConsumer = new FlinkKafkaConsumer<String>("order", new SimpleStringSchema(), properties);
        orderConsumer.setStartFromEarliest();
        SingleOutputStreamOperator<Tuple4<Long, Long, Long, Double>> orderDS = env
                .addSource(orderConsumer)
                .map(new MapFunction<String, Tuple4<Long, Long, Long, Double>>() {

                    @Override
                    public Tuple4<Long, Long, Long, Double> map(String value) throws Exception {
                        String[] arr = value.split(",");
                        System.out.println(" line ---------------> " + value);
                        return new Tuple4<Long, Long, Long, Double>(Long.valueOf(arr[0].trim()),
                                                                    Long.valueOf(arr[1].trim()),
                                                                    Long.valueOf(arr[2].trim()),
                                                                    Double.valueOf(arr[3].trim()));
                    }
                });

        // register table
        Table orderTab = tableEnv.fromDataStream(orderDS, "order_id, user_id, product_id, price");
        tableEnv.registerTable("my_order", orderTab);

        /*
         * Description:
         * <pre>
         *     Select *
         * </pre>
         * @param [args]
         * @return void
         */
        Table retTab = tableEnv.sqlQuery("select * from my_order");
        retTab.printSchema();
        DataStream ds = tableEnv.toRetractStream(retTab, Row.class);

        ds.addSink(new SinkFunction() {

            @Override
            public void invoke(Object value,
                               Context context) throws Exception {
                System.out.println("tuple all table ----->" + value.toString());
            }
        });

        /*
         * Description:
         * <pre>
         *     Where 条件
         * </pre>
         * @param [args]
         * @return void
         */
        Table orderIdTab = tableEnv.sqlQuery("select * from my_order where order_id = 1");
        DataStream orderIdDS = tableEnv.toRetractStream(orderIdTab,Row.class);
        orderIdDS.addSink(new SinkFunction() {

            @Override
            public void invoke(Object value,
                               Context context) throws Exception {
                System.out.println("tuple where id ----->" + value.toString());
            }
        });

        try {
            env.execute("StreamingFlinkSQL2Join");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
