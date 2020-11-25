package com.zk.kafka2hbase;



import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zk.utils.ConsumerRecordKafkaDeserializationSchema;
import com.zk.utils.HbaseUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.commons.collections.IteratorUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
/**
 * @author zhangkai
 * @create 2020/1/15
 */
public class Kafka2hbaseDemo {

    public static void main(String[] args) throws Exception{

        String topicList = "otter_zktest";
        int mapNum = 4;
        int numTwo = 4;
        String hbaseNameSpace = "test";


//        if(args.length!=3){
//            System.out.println(" 请输入3个参数，源topic，map-partition，目标topic");
//            System.exit(1);
//        }

        System.out.println(args.length);
        System.out.println(topicList);

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置重启策略
//        see.setRestartStrategy(RestartStrategies.fixedDelayRestart(30, 5000));
//
//        see.setStateBackend((StateBackend)new RocksDBStateBackend("hdfs://HDFS80377/user/zhangkai/flink-checkpoints/kafka2kafka"));//
//        CheckpointConfig checkpointConfig = see.getCheckpointConfig();
//        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        checkpointConfig.setCheckpointInterval(30000);
//        checkpointConfig.setMaxConcurrentCheckpoints(3);
//        checkpointConfig.setCheckpointTimeout(60000);

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.2.40.10:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");//value 反序列化
        //动态感知kafka主题分区的增加  单位毫秒
        props.setProperty("flink.partition-discovery.interval-millis", "5000");


        FlinkKafkaConsumer<ConsumerRecord> topicData = new FlinkKafkaConsumer<>(Arrays.asList(topicList.split(",")), new ConsumerRecordKafkaDeserializationSchema(), props);
        topicData.setStartFromLatest();


        DataStreamSource<ConsumerRecord> consumerRecordDataStreamSource = see.addSource(topicData).setParallelism(mapNum);
        DataStream<JSONObject> jsonStreamOut = consumerRecordDataStreamSource.map(new MapFunction<ConsumerRecord, JSONObject>() {
            @Override
            public JSONObject map(ConsumerRecord consumerRecord) throws Exception {
                String value = new String((byte[]) consumerRecord.value(),StandardCharsets.UTF_8);
                String[] splits = value.split("#%&", 2);
                JSONObject jsonObject = JSON.parseObject(splits[1]);
                String[] dbtable = splits[0].split("#@#");
                jsonObject.put("dbName",dbtable[0]);
                jsonObject.put("tableName",dbtable[1]);
                String hbaseTable = splits[0].replaceAll("#@#","_").replaceAll("_\\d+","").replaceAll("\\d+","");
                jsonObject.put("hTableName",hbaseTable);
                String rowkey = getRowKey(jsonObject.getJSONArray("keys"),splits[0]);
                jsonObject.put("hbaseRowKeyTimestamp",consumerRecord.timestamp());
                jsonObject.put("hbaseRowKey",rowkey);
                jsonObject.put("partitionNum",consumerRecord.partition());
                return jsonObject;
            }
        }).setParallelism(mapNum);


        KeyedStream<JSONObject,String> processHbase = jsonStreamOut.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                String hbaseRowKey = jsonObject.getString("partitionNum");
                return hbaseRowKey;
            }
        });

        processHbase.window(TumblingProcessingTimeWindows.of(Time.milliseconds(2500)))

                .process(new ProcessWindowFunction<JSONObject, List<JSONObject>, String, TimeWindow>() {

                    private Connection connection = null;
                    private List<TableName> allTables = null;
                    HashMap<String,BufferedMutator> mutatorMap = new HashMap<String,BufferedMutator>();
                    HashMap<String,ArrayList<Mutation>> tableMutateMap = new HashMap<String,ArrayList<Mutation>>();
                    HbaseUtil hbaseUtil = new HbaseUtil();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        connection = hbaseUtil.getCon("10.2.40.12,10.2.40.11,10.2.40.8", "2181");

                    }
                    @Override
                    public void process(String keyBy, Context context, Iterable<JSONObject> iterable, Collector<List<JSONObject>> out) throws Exception {
                        allTables = hbaseUtil.getAllTables();

                        Iterator<JSONObject> iterator = iterable.iterator();
                        List<JSONObject> logList = IteratorUtils.toList(iterator);

//                        System.out.println("  ######## logList.size is "+logList.size());
                        logList.stream().forEach(logJson -> {

                            System.out.println("###### data is "+logJson);

                            String hTableName = logJson.getString("hTableName");
                            String recentHTableName = hbaseNameSpace + ":recent_" + hTableName;
                            String currentHTableName = hbaseNameSpace + ":" + hTableName;

                            mutatorMapPut(mutatorMap,recentHTableName,connection);
                            mutatorMapPut(mutatorMap,currentHTableName,connection);

                            String rowkey = logJson.getString("hbaseRowKey");
                            long hbaseRowKeyTimestamp = logJson.getLong("hbaseRowKeyTimestamp");

                            String event_type = logJson.getString("type");
                            if(event_type.equalsIgnoreCase("DELETE")){

                                Delete delete = new Delete(Bytes.toBytes(rowkey),hbaseRowKeyTimestamp);
                                mutatorMutate(allTables,recentHTableName,delete,tableMutateMap);
                                mutatorMutate(allTables,currentHTableName,delete,tableMutateMap);
                            }else {
                                Put put = new Put(rowkey.getBytes(),hbaseRowKeyTimestamp);
                                Set<String> set = logJson.keySet();
                                set.stream().forEach(key -> {
                                    String value = logJson.getString(key);
                                    if(value.startsWith("[")&&value.endsWith("]")){
                                        JSONArray jsonArray = JSONObject.parseArray(value);
                                        for(int i=0;i<jsonArray.size();i++){
                                            JSONObject jsonObject = jsonArray.getJSONObject(i);
                                            String name = jsonObject.getString("name");
                                            String true_value = jsonObject.getString("value");
                                            put.addColumn("info".getBytes(), name.getBytes(), true_value.getBytes());
                                        }
                                    }else if(value.equalsIgnoreCase("UPDATE")) {
                                        put.addColumn("info".getBytes(), key.getBytes(), logJson.getString(key).getBytes());
                                        put.addColumn("info".getBytes(), "ods_update_time".getBytes(), getCurrentTime().getBytes());
                                    }else if(value.equalsIgnoreCase("INSERT")) {
                                        put.addColumn("info".getBytes(), key.getBytes(), logJson.getString(key).getBytes());
                                        put.addColumn("info".getBytes(), "ods_create_time".getBytes(), getCurrentTime().getBytes());
                                        put.addColumn("info".getBytes(), "ods_update_time".getBytes(), getCurrentTime().getBytes());
                                    }else {
                                        put.addColumn("info".getBytes(), key.getBytes(), logJson.getString(key).getBytes());
                                    }
                                });
                                mutatorMutate(allTables,recentHTableName,put,tableMutateMap);
                                mutatorMutate(allTables,currentHTableName,put,tableMutateMap);
                            }
                        });

//                        System.out.println(" ####### tableMutateMap size is " + tableMutateMap.size());
                        Set<String> tableMutateMapKeySet = tableMutateMap.keySet();
                        for (String tableName : tableMutateMapKeySet){
                            mutatorMap.get(tableName).mutate(tableMutateMap.get(tableName));
                        }

//                        System.out.println(" ####### mutatorMap size is " + mutatorMap.size());
                        Set<String> mutatorMapKeySet = mutatorMap.keySet();
                        for(String table : mutatorMapKeySet){
                            BufferedMutator bufferedMutator = mutatorMap.get(table);
                            bufferedMutator.flush();
                            bufferedMutator.close();
                        }

                        tableMutateMap.clear();
                        mutatorMap.clear();

                    }
                }).setParallelism(numTwo);

        see.execute("kafka 2 hbase");

    }

    public static String getRowKey(JSONArray keys, String dbTable){
        String rowKey = "";

        HashMap<String, String> analysis = getAnalysis(keys);

        String[] dbtabnew =dbTable.split("#@#");
        String db_old = dbtabnew[0].replaceAll("_","");
        String table_old = dbtabnew[1].replaceAll("_","");

        if(!analysis.containsKey("id")){
            rowKey = new StringBuilder(StringUtils.join(analysis.values(),"")).reverse().toString();
            if(rowKey==null || rowKey.equals("")){
                rowKey = new StringBuilder(StringUtils.join(analysis.keySet(),"")).reverse().toString();
            }
        }else {
            StringBuilder rkms = new StringBuilder(analysis.get("id"));
            if(db_old.matches("[a-zA-Z]{0,}[0-9]{1,}[a-zA-Z]{0,}")&&table_old.matches("[a-zA-Z]{0,}[0-9]{1,}[a-zA-Z]{0,}")){
                rowKey =  rkms.reverse().toString() + "_" + table_old.replaceAll("[a-zA-Z]{1,}","") + "_" + db_old.replaceAll("[a-zA-Z]{1,}","");
            }else if (!db_old.matches("[a-zA-Z]{0,}[0-9]{1,}[a-zA-Z]{0,}")&&table_old.matches("[a-zA-Z]{0,}[0-9]{1,}[a-zA-Z]{0,}")){
                rowKey =  rkms.reverse().toString() + "_" + table_old.replaceAll("[a-zA-Z]{1,}","");
            }else{
                rowKey = rkms.reverse().toString();
                if(rowKey.equals("")||rowKey == null){
                    rowKey = new StringBuilder(StringUtils.join(analysis.keySet(),"")).reverse().toString();
                }
            }
        }
        return rowKey;
    }

    public static HashMap<String,String> getAnalysis(JSONArray keys){
        HashMap<String, String> hashMap = new HashMap<>();
        for (int i=0;i<keys.size();i++){
            JSONObject jsonObject = keys.getJSONObject(i);
            String name = jsonObject.getString("name");
            String value = jsonObject.getString("value");
            hashMap.put(name,value);
        }
        return  hashMap;
    }



    /**
     * mutatorMap 集合里保存 HTable和bufferMutator的对应关系
     * */
    public static void mutatorMapPut(HashMap<String,BufferedMutator> mutatorMap, String hTableName,Connection connection){
        try {
            if(!mutatorMap.containsKey(hTableName)){
                BufferedMutatorParams bufferedMutatorParams = new BufferedMutatorParams(TableName.valueOf(hTableName)).writeBufferSize(5 * 1024 * 1024);
                BufferedMutator bufferedMutator = connection.getBufferedMutator(bufferedMutatorParams);
                mutatorMap.put(hTableName,bufferedMutator);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * map集合
     * 每个表对应一个list集合，集合是put和delete操作
     * */
    public static void mutatorMutate(List<TableName> allHTables, String hTableName,Mutation putOrDelete ,HashMap<String,ArrayList<Mutation>> tableMatateMap ){
        if(allHTables.contains(TableName.valueOf(hTableName))){
            ArrayList<Mutation> listMutation = tableMatateMap.getOrDefault(hTableName, new ArrayList<Mutation>());
            listMutation.add(putOrDelete);
            tableMatateMap.put(hTableName,listMutation);
        }

    }

    public static String  getCurrentTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String curTime = dateFormat.format(System.currentTimeMillis());
        return curTime;
    }
}
