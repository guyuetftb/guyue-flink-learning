package com.guyue.examples;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lipeng com.guyue.flink lipeng 2019/4/18
 */
public class StreamingDimensionMySql23 {

	public static Logger logger = LoggerFactory.getLogger(StreamingDimensionMySql23.class);

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
		DataStream<Long> dataStream = env.socketTextStream(host, port).map(x -> {
			return Long.valueOf(x);
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

		RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldsTypes);

		DataStream<Row> resultDataStream = AsyncDataStream
			.orderedWait(dataStream, new AsyncDimensionMySqlRequest(jdbcConnEntity), 1000, TimeUnit.MILLISECONDS, 100);

		resultDataStream.addSink(new SinkFunction<Row>() {

			@Override
			public void invoke(Row row,
							   Context context) throws Exception {
				logger.info(row.toString());
			}
		});

		try {
			env.execute("StreamingDimensionMySql22");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static class AsyncDimensionMySqlRequest extends RichAsyncFunction<Long, Row> {

		private final static Logger asyncLogger = LoggerFactory.getLogger(AsyncDimensionMySqlRequest.class);

		private JDBCConnEntity jdbcConnEntity = null;
		private String queryTemplate = "select * from user_info where id = ?";
		private transient SQLClient mySQLClient;

		public AsyncDimensionMySqlRequest() {
			super();
		}

		public AsyncDimensionMySqlRequest(String drivername,
										  String dbURL,
										  String username,
										  String password) {
			this();
			this.jdbcConnEntity = new JDBCConnEntity();
			jdbcConnEntity.setDriver(drivername);
			jdbcConnEntity.setUserName(username);
			jdbcConnEntity.setDbUrl(dbURL);
			jdbcConnEntity.setPassword(password);

			asyncLogger.info("-----------> init(String drivername,String dbURL,String username,String password) " + jdbcConnEntity);
		}

		public AsyncDimensionMySqlRequest(JDBCConnEntity jdbcConnEntity) {
			this();
			this.jdbcConnEntity = jdbcConnEntity;
			asyncLogger.info("-----------> init(JDBCConnEntity jdbcConnEntity) " + jdbcConnEntity);
		}

		@Override
		public void open(Configuration configuration) throws Exception {
			asyncLogger.info("-----------> open(Configuration configuration) " + jdbcConnEntity);
			try {
				JsonObject mySQLClientConfig = new JsonObject();
				mySQLClientConfig.put("url", jdbcConnEntity.getDbUrl())
					.put("driver_class", jdbcConnEntity.getDriver())
					.put("max_pool_size", 20)
					.put("user", jdbcConnEntity.getUserName())
					.put("password", jdbcConnEntity.getPassword());

				VertxOptions vo = new VertxOptions();
				vo.setEventLoopPoolSize(10);
				vo.setWorkerPoolSize(20);
				Vertx vertx = Vertx.vertx(vo);
				mySQLClient = JDBCClient.createNonShared(vertx, mySQLClientConfig);
			} catch (Exception e) {
				throw new IllegalArgumentException("open() failed." + e.getMessage(), e);
			}
		}

		@Override
		public void close() throws Exception {
			asyncLogger.info("-----------> close() " + jdbcConnEntity);
			mySQLClient.close();
		}

		@Override
		public void asyncInvoke(Long key,
								ResultFuture<Row> resultFuture) throws Exception {

			mySQLClient.getConnection(conn -> {
				if (conn.failed()) {
					return;
				}

				final SQLConnection sqlConnection = conn.result();
				sqlConnection.query("select id, user_name, sex from user_info where id = " + key, res2 -> {
					if (res2.succeeded()) {
						ResultSet resultSet = res2.result();
						List<Row> rowList = Lists.newArrayList();
						List<String> columns = resultSet.getColumnNames();

						for (JsonObject rows : resultSet.getRows()) {
							Row row = new Row(columns.size());
							for (int index = 0; index < columns.size(); index++) {
								row.setField(index, rows.getValue(columns.get(index)));
							}
							rowList.add(row);
						}

						resultFuture.complete(rowList);
					}
				});
			});
		}
	}

	private static class JDBCConnEntity implements Serializable {

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

		@Override
		public String toString() {
			return "JDBCConnEntity{" +
				"driver='" + driver + '\'' +
				", dbUrl='" + dbUrl + '\'' +
				", userName='" + userName + '\'' +
				", password='" + password + '\'' +
				", dbType='" + dbType + '\'' +
				'}';
		}
	}
}
