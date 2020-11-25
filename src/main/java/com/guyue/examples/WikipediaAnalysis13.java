package com.guyue.examples;

/**
 * Created by lipeng
 * wikiedits
 * lipeng
 * 2019/3/15
 */
public class WikipediaAnalysis13 {
/**
    public static void main(String[] args) {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream edits = see.addSource(new WikipediaEditsSource());

        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy(new KeySelector<WikipediaEditEvent,String>(){
            @Override
           public String getKey(WikipediaEditEvent event){
                return event.getUser();
           }
        });

        DataStream<Tuple2<String,Long>> result = keyedEdits
                .timeWindow(Time.seconds(5))
                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {

                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> acc,
                                                     WikipediaEditEvent event) throws Exception {
                        acc.f0 = event.getUser();
                        acc.f1 += event.getByteDiff();
                        return acc;
                    }
                });
        result.print();
        try {
            see.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
 */
}
