package com.guyue.examples;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * Created by lipeng
 * com.guyue.flink
 * lipeng
 * 2019/3/12
 */
public class StreamSQLExample12 {

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        DataStream<Order> orderA = environment.fromCollection(Arrays.asList(new Order(1l, "beer", 3),
                                                                            new Order(1L, "diaper", 4),
                                                                            new Order(3L, "rubber", 2)));

        DataStream<Order> orderB = environment.fromCollection(Arrays.asList(
                new Order(1L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(3L, "beer", 1)));


        Table tableA = tableEnvironment.fromDataStream(orderA, "user, product, amount");
        Table tableB = tableEnvironment.fromDataStream(orderB, "user, product, amount");
        Table result = tableEnvironment.sqlQuery("select * from " + tableA + " t1 where amount > 2 UNION ALL select * from  " + tableB + " t2 where amount < 2");
        tableEnvironment.toAppendStream(result, Order.class).print();

        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Simple POJO.
     */
    public static class Order {
        public Long   user;
        public String product;
        public int    amount;

        public Order() {
        }

        public Order(Long user,
                     String product,
                     int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }
}
