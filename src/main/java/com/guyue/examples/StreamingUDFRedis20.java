package com.guyue.examples;

import com.guyue.flink.udf.MYHashCode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lipeng
 * com.guyue.flink
 * lipeng
 * 2019/4/8
 */
public class StreamingUDFRedis20 {

    public static void main(String[] args) {

        //定义socket的端口号
        String host = "localhost";
        int port = 19999;

        Map<String, String> redisMap = new HashMap<String, String>();
        redisMap.put("redis.host", "localhost");
        redisMap.put("redis.port", "6379");

        ParameterTool parameterTool = ParameterTool.fromMap(redisMap);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置全局参数
        env.getConfig().setGlobalJobParameters(parameterTool);

        String tableName = "wordCount";
        TypeInformation[] typeInfos = new TypeInformation[2];
        typeInfos[0] = Types.STRING;
        typeInfos[1] = Types.INT;
        String[] fields = new String[]{"word", "frequency"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInfos, fields);

        DataStream<String> sourceDS = env.socketTextStream(host, port, "\n");
        SingleOutputStreamOperator<Row> singleRow = sourceDS.map(new MapFunction<String, Row>() {

            @Override
            public Row map(String line) throws Exception {
                String[] strArr = line.split(",");
                Row row = new Row(2);
                row.setField(0, strArr[0]);
                row.setField(1, Integer.valueOf(strArr[1]));
                return row;
            }
        }).returns(rowTypeInfo);

        tableEnv.registerDataStream(tableName, singleRow);
        tableEnv.registerFunction("hashCode", new MYHashCode());
        Table table = tableEnv.sqlQuery("select word, hashCode(word) from " + tableName);
        DataStream<Tuple2<Boolean, Row>> result = tableEnv.toRetractStream(table, Row.class);
        result.print();

        try {
            env.execute("StreamingUDFRedis20");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
