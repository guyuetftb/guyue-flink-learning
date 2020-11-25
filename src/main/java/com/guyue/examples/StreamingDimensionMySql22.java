package com.guyue.examples;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * Created by lipeng
 * com.guyue.flink
 * lipeng
 * 2019/4/18
 */
public class StreamingDimensionMySql22 {

    public static Logger logger = Logger.getLogger(StreamingDimensionMySql22.class);

    public static void main(String[] args) {
        int port = -1;
        String host = null;

        try {
            final ParameterTool parameterTool = ParameterTool.fromArgs(args);
            host = parameterTool.get("host", "localhost");
            port = parameterTool.getInt("port", 9999);
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'WordCountSocketDataSet --host <ip> --port <port>'");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> leftDataStream = env.socketTextStream(host, port).map(x -> {
            return Integer.valueOf(x);
        });

        TypeInformation<?>[] fieldsTypes = new TypeInformation<?>[]{
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO
        };

        JDBCConnEntity jdbcConnEntity = new JDBCConnEntity();
        jdbcConnEntity.setDbType("mysql");
        jdbcConnEntity.setDriver("com.mysql.jdbc.Driver");
        jdbcConnEntity.setDbUrl("jdbc:mysql://localhost:3306/flink");
        jdbcConnEntity.setUserName("root");
        jdbcConnEntity.setPassword("123456abc");

        String sql = "select * from user_info";

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldsTypes);
        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(jdbcConnEntity.getDriver())
                .setUsername(jdbcConnEntity.getUserName())
                .setPassword(jdbcConnEntity.getPassword())
                .setDBUrl(jdbcConnEntity.getDbUrl())
                .setQuery(sql)
                .setRowTypeInfo(rowTypeInfo)
                .finish();

        StreamExecutionEnvironment dimensionMySqlEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> dimensionMySqlSource = dimensionMySqlEnv.createInput(jdbcInputFormat);

        StreamTableEnvironment dimensionMySqlTabEnv = StreamTableEnvironment.create(dimensionMySqlEnv);
        dimensionMySqlTabEnv.registerDataStream("mysql", dimensionMySqlSource, "id1,user,age");
        dimensionMySqlTabEnv.registerDataStream("stream1", leftDataStream, "id2");
        Table mysql = dimensionMySqlTabEnv.sqlQuery("select * from mysql");

        Table stream1 = dimensionMySqlTabEnv.scan("stream1");
        Table joind = stream1.join(mysql, "id1=id2");

        DataStream<Row> res = dimensionMySqlTabEnv.toAppendStream(joind, Row.class);

        res.addSink(new SinkFunction<Row>() {

            @Override
            public void invoke(Row row, Context context) throws Exception {
                logger.info(row.toString());
            }
        });

        try {
            env.execute("StreamingDimensionMySql22");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class JDBCConnEntity implements Serializable {

        private String driver;
        private String dbUrl;
        private String userName;
        private String password;
        private String dbType;

        public String getDriver() {
            return driver;
        }

        public void setDriver(String driver) {
            this.driver = driver;
        }

        public String getDbUrl() {
            return dbUrl;
        }

        public void setDbUrl(String dbUrl) {
            this.dbUrl = dbUrl;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getDbType() {
            return dbType;
        }

        public void setDbType(String dbType) {
            this.dbType = dbType;
        }
    }
}
