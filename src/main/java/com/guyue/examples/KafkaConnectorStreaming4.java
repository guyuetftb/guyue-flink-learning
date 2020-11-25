package com.guyue.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lipeng
 * wikiedits
 * lipeng
 * 2019/3/15
 */
public class KafkaConnectorStreaming4 {

    private static Logger LOG = LoggerFactory.getLogger(KafkaConnectorStreaming4.class.getName());

    /**

    public static void main(String[] args) {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream edits = see.addSource(new WikipediaEditsSource());

        KeyedStream<WikipediaEditEvent, String> keyedStream = edits.keyBy(new KeySelector<WikipediaEditEvent, String>() {

            @Override
            public String getKey(WikipediaEditEvent wikipediaEditEvent) throws Exception {
                LOG.info(" --------> getKey.user" + wikipediaEditEvent.getUser() + ", time=" + wikipediaEditEvent.getTimestamp());
                return wikipediaEditEvent.getUser();
            }
        });

        DataStream<Tuple2<String, Long>> result = keyedStream
                .timeWindow(Time.seconds(5))
                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {

                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> acc,
                                                     WikipediaEditEvent event) throws Exception {
                        acc.f0 = event.getUser();
                        acc.f1 += event.getByteDiff();
                        LOG.info(" --------> acc.f0 = " + acc.f0 + ", acc.f1 = " + acc.f1 +", time=" + event.getTimestamp());
                        LOG.info(" --------> event.user = " + event.getUser() + ", time=" + event.getTimestamp());
                        return acc;
                    }
                });

        result.map(new MapFunction<Tuple2<String, Long>, String>() {

            @Override
            public String map(Tuple2<String, Long> stringLongTuple2) throws Exception {
                LOG.info(" --------> result = " + stringLongTuple2.toString());
                return stringLongTuple2.toString();
            }
        }).addSink(new FlinkKafkaProducer010<String>("localhost:9092", "wiki-result", new SimpleStringSchema()));

        try {
            see.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
     */
}
