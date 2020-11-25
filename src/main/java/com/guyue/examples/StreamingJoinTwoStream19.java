package com.guyue.examples;

import com.guyue.utils.KafkaUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class StreamingJoinTwoStream19 {

    public static void main(String[] args) {

        /**
         * Kafka Properties
         */
        String transactionTopic = "transaction";
        String transactionGroupId = transactionTopic + "1";
        String marketTopic = "market";
        String marketGroupId = marketTopic + "1";
        Properties transactionProperties = KafkaUtils.buildKafkaProperties(transactionGroupId, transactionTopic, "earliest");
        Properties marketProperties = KafkaUtils.buildKafkaProperties(marketGroupId, marketTopic, "earliest");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        System.out.println("---------------------------------------------------------------------- 1 ");
        /**
         * Kafka Consumer
         */
        FlinkKafkaConsumer transactionConsumer = new FlinkKafkaConsumer(transactionTopic, new SimpleStringSchema(), transactionProperties);
        transactionConsumer.setStartFromLatest();
        // transactionConsumer.setStartFromGroupOffsets();
        SingleOutputStreamOperator<Tuple4<Long, Long, Long, Long>> transactionKafkaStream = env.addSource(transactionConsumer)
                .setParallelism(1)
                .map(new TransactoinPriceMapFunction())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple4<Long, Long, Long, Long>>() {

                    long currentMaxTimestamp = 0L;
                    long maxOutOfOrderness = 2000L;
                    Watermark watermark = null;

                    //最大允许的乱序时间是10s
                    @Override
                    public Watermark getCurrentWatermark() {
                        watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                        return watermark;
                    }

                    @Override
                    public long extractTimestamp(Tuple4<Long, Long, Long, Long> element,
                                                 long previousElementTimestamp) {
                        currentMaxTimestamp = Math.max(element.f1, previousElementTimestamp);
                        System.out.println(" Assigner - extractTimestamp - transaction = " + element.f1 + ", previousElementTimestamp = " + previousElementTimestamp);
                        return element.f1;
                    }
                });

        transactionKafkaStream.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple4<Long, Long, Long, Long>>() {
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(Tuple4<Long, Long, Long, Long> lastElement, long extractedTimestamp) {
                return null;
            }

            @Override
            public long extractTimestamp(Tuple4<Long, Long, Long, Long> element, long previousElementTimestamp) {
                return 0;
            }
        });

        System.out.println("---------------------------------------------------------------------- 2 ");
        FlinkKafkaConsumer marketConsumer = new FlinkKafkaConsumer(marketTopic, new SimpleStringSchema(), marketProperties);
        marketConsumer.setStartFromLatest();
        // marketConsumer.setStartFromGroupOffsets();
        SingleOutputStreamOperator<Tuple3<Long, Long, Long>> marketKafkaStream = env.addSource(marketConsumer)
                .setParallelism(1)
                .map(new MarketMapFunction())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<Long, Long, Long>>() {

                    long currentMaxTimestamp = 0L;
                    long maxOutOfOrderness = 2000L;
                    Watermark watermark = null;

                    //最大允许的乱序时间是10s
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                        return watermark;
                    }

                    @Override
                    public long extractTimestamp(Tuple3<Long, Long, Long> element,
                                                 long previousElementTimestamp) {
                        currentMaxTimestamp = Math.max(element.f1, currentMaxTimestamp);
                        System.out.println(" Assigner - extractTimestamp - market " + element.f1 + ", previousElementTimestamp = " + previousElementTimestamp);
                        return element.f1;
                    }
                });

//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Long, Long>>() {
//
//                    @Override
//                    public long extractAscendingTimestamp(Tuple3<Long, Long, Long> m) {
//                        System.out.println(" ++++++++++++ >  tuple3 " + m.f1);
//                        return m.f1;
//                    }
//                });

        System.out.println("---------------------------------------------------------------------- 3");

        DataStream joinedStream = transactionKafkaStream
                .join(marketKafkaStream)        // Join a streaming
                .where(new KeySelector<Tuple4<Long, Long, Long, Long>, Long>() {

                    @Override
                    public Long getKey(Tuple4<Long, Long, Long, Long> tuple4) throws Exception {
                        System.out.println("-------------- transaction - szCode = " + tuple4.f0);
                        return tuple4.f0;    // szCode
                    }
                }) // assign where
                .equalTo(new KeySelector<Tuple3<Long, Long, Long>, Long>() {

                    @Override
                    public Long getKey(Tuple3<Long, Long, Long> tuple3) throws Exception {
                        System.out.println("-------------- market - szCode = " + tuple3.f0);
                        return tuple3.f0;   // szCode
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Tuple4<Long, Long, Long, Long>, Tuple3<Long, Long, Long>, Tuple6<Long, String, String, Long, Long, Long>>() {

                    @Override
                    public Tuple6<Long, String, String, Long, Long, Long> join(Tuple4<Long, Long, Long, Long> transaction,
                                                                               Tuple3<Long, Long, Long> market) throws Exception {

                        System.out.println("-------------- result - szCode " + transaction.f0);
                        Long transactionTime = transaction.f1;
                        Long marketTime = market.f1;
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        return new Tuple6<>(transaction.f1, format.format(marketTime) + ":marketTime ", format.format(transactionTime) + ":transactionTime", market.f2, transaction.f2, transaction.f3);
                        // if (differ >= 0 && differ <= 3 * 1000) {
                        // }
                    }
                });
        joinedStream.print();

        try {
            env.execute("GuYue 2 DataStream join");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class TransactoinPriceMapFunction implements MapFunction<String, Tuple4<Long, Long, Long, Long>> {

        @Override
        public Tuple4<Long, Long, Long, Long> map(String line) throws Exception {
            String[] columns = line.split(",");

            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");

            Transaction transaction = new Transaction(columns[0],       // columns(0)
                                                      Long.valueOf(columns[1]),     // columns(1).toLong
                                                      columns[2],       // columns(2)
                                                      columns[3],       // columns(3)
                                                      Long.valueOf(columns[4]), // columns(4).toLong
                                                      Long.valueOf(columns[5]), // columns(5).toLong
                                                      Long.valueOf(columns[6]), // columns(6).toLong
                                                      Long.valueOf(columns[7]), // columns(7).toLong
                                                      Long.valueOf(columns[8]), // columns(8).toLong
                                                      Integer.valueOf(columns[9]),  // columns(9).toInt
                                                      columns[9],  // columns(9)
                                                      columns[10], // columns[10]
                                                      Long.valueOf(columns[11]),    // columns(11).toLong
                                                      Long.valueOf(columns[12]),    // columns(12).toLong
                                                      Long.valueOf(columns[13])     // columns(13).toLong
            );
            Long eventTime = 0l;
            if (transaction.nTime.length() == 8) {
                String eventTimeString = transaction.nAction + '0' + transaction.nTime;
                eventTime = format.parse(eventTimeString).getTime();
            } else {
                String eventTimeString = transaction.nAction + transaction.nTime;
                eventTime = format.parse(eventTimeString).getTime();
            }

            System.out.println(" -> TransactoinPriceMapFunction = " + line + ", event_time = " + eventTime);

            return new Tuple4(transaction.szCode, eventTime, transaction.nPrice, transaction.nTurnover);
        }
    }

    static class MarketMapFunction implements MapFunction<String, Tuple3<Long, Long, Long>> {

        @Override
        public Tuple3<Long, Long, Long> map(String line) throws Exception {
            String[] columns = line.split(",");
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
            MarketInfo marketInfo = new MarketInfo(Long.valueOf(columns[0]), columns[1], columns[2], Long.valueOf(columns[3]));

            Long eventTime = null;
            if (marketInfo.nTime.length() == 8) {
                String tmpEventTime = marketInfo.nActionDay + '0' + marketInfo.nTime;
                eventTime = format.parse(tmpEventTime).getTime();
            } else {
                String tmpEventTime = marketInfo.nActionDay + marketInfo.nTime;
                eventTime = format.parse(tmpEventTime).getTime();
            }

            System.out.println(" -> MarketMapFunction = " + line + ", event_time = " + eventTime);
            return new Tuple3(marketInfo.szCode, eventTime, marketInfo.nMatch);
        }
    }

    static class Transaction {

        public String  szWindCode;
        public Long    szCode;
        public String  nAction;
        public String  nTime;
        public Long    seq;
        public Long    nIndex;
        public Long    nPrice;
        public Long    nVolume;
        public Long    nTurnover;
        public Integer nBSFlag;
        public String  chOrderKind;
        public String  chFunctionCode;
        public Long    nAskOrder;
        public Long    nBidOrder;
        public Long    localTime;

        public Transaction(String szWindCode,
                           Long szCode,
                           String nAction,
                           String nTime,
                           Long seq,
                           Long nIndex,
                           Long nPrice,
                           Long nVolume,
                           Long nTurnover,
                           Integer nBSFlag,
                           String chOrderKind,
                           String chFunctionCode,
                           Long nAskOrder,
                           Long nBidOrder,
                           Long localTime) {
            this.szWindCode = szWindCode;
            this.szCode = szCode;
            this.nAction = nAction;
            this.nTime = nTime;
            this.seq = seq;
            this.nIndex = nIndex;
            this.nPrice = nPrice;
            this.nVolume = nVolume;
            this.nTurnover = nTurnover;
            this.nBSFlag = nBSFlag;
            this.chOrderKind = chOrderKind;
            this.chFunctionCode = chFunctionCode;
            this.nAskOrder = nAskOrder;
            this.nBidOrder = nBidOrder;
            this.localTime = localTime;
        }
    }

    static class MarketInfo {
        public Long   szCode;
        public String nActionDay;
        public String nTime;
        public Long   nMatch;

        public MarketInfo(Long szCode,
                          String nActionDay,
                          String nTime,
                          Long nMatch) {
            this.szCode = szCode;
            this.nActionDay = nActionDay;
            this.nTime = nTime;
            this.nMatch = nMatch;
        }
    }

    static class Market {
        public Long   szCode;
        public String eventTime;
        public Long   nMatch;

        public Market(Long szCode,
                      String eventTime,
                      Long nMatch) {
            this.szCode = szCode;
            this.eventTime = eventTime;
            this.nMatch = nMatch;
        }
    }
}
