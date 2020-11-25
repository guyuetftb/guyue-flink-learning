package com.guyue.flink.duoyi.examples.dimension.function;

import com.alibaba.druid.pool.DruidDataSource;
import com.guyue.flink.duoyi.examples.dimension.bean.ActivityBean;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

/**
 * @ClassName DataToActivityBeanMySqlFunction
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-06 20:20
 */
public class DataToActivityBeanAsyncMySqlFunction extends RichAsyncFunction<String, ActivityBean> {

	private String driver = "com.mysql.jdbc.Driver";
	private String url = "jdbc:mysql://127.0.0.1:3306/test_master?characterEncoding=utf8&useSSL=false&allowMultiQueries=true";
	private String user = "root";
	private String password = "123456abc";

	private transient DruidDataSource druidDataSource = null;
	private transient ExecutorService executorService = null;

	@Override
	public void open(Configuration parameters) throws Exception {
		System.out.println(Thread.currentThread().getName() + ", timestamp = " + System.currentTimeMillis() + ", open() ");
		super.open(parameters);

		// 线程池
		executorService = Executors.newFixedThreadPool(5);

		// Data Source
		druidDataSource = new DruidDataSource();
		druidDataSource.setDriverClassName(driver);
		druidDataSource.setUsername(user);
		druidDataSource.setUrl(url);
		druidDataSource.setPassword(password);
		druidDataSource.setMaxActive(5);
		druidDataSource.setInitialSize(5);
		druidDataSource.setMinIdle(2);
	}

	@Override
	public void close() throws Exception {
		super.close();
		System.out.println(Thread.currentThread().getName() + ", timestamp = " + System.currentTimeMillis() + ", close() ");
		executorService.shutdown();
		druidDataSource.close();
	}

	@Override
	public void timeout(String input, ResultFuture<ActivityBean> resultFuture) throws Exception {
		System.out.println(Thread.currentThread().getName() + ", timestamp = " + System.currentTimeMillis() + ", input = " + input + ", timeout()");
	}

	@Override
	public void asyncInvoke(String line, ResultFuture<ActivityBean> resultFuture) throws Exception {

		// u001,A1,2019-09-02 10:10:11,1,北京市
		String[] lineArr = line.split(",");
		String activityName = null;
		String uid = lineArr[0];
		String aid = lineArr[1];
		String time = lineArr[2];
		int eventType = Integer.parseInt(lineArr[3]);
		String province = lineArr[4];

		Future<String> nameFuture = executorService.submit(new Callable<String>() {

			@Override
			public String call() throws Exception {
				System.out.println(Thread.currentThread().getName() + ", timestamp = " + System.currentTimeMillis() + ", Callable.call()");
				return queryFromMySql(aid);
			}
		});

		CompletableFuture.supplyAsync(new Supplier<String>() {
			@Override
			public String get() {
				try {
					System.out.println(Thread.currentThread().getName() + ", timestamp = " + System.currentTimeMillis() + ", Supplier.get()");
					return nameFuture.get();
				} catch (Exception e) {
					return "ERROR-null";
				}
			}
		}).thenAccept(new Consumer<String>() {
			@Override
			public void accept(String activityName) {
				System.out.println(Thread.currentThread().getName() + ", timestamp = " + System.currentTimeMillis() + ", Consumer.get()");
				resultFuture.complete(Collections.singleton(ActivityBean.of(uid, aid, activityName, time, eventType, province)));
			}
		});
	}

	public String queryFromMySql(String aid) throws SQLException {

		String sql = "select name from t_activities where id = ?";
		Connection connection = null;
		PreparedStatement statement = null;
		String activityName = null;
		try {
			connection = druidDataSource.getConnection();
			statement = connection.prepareStatement(sql);
			statement.setString(1, aid);
			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {
				activityName = resultSet.getString("name");
			}
		} finally {
			connection.close();
		}
		System.out.println(Thread.currentThread().getName() + ", timestamp = " + System.currentTimeMillis()
			+ ", queryFromMySql(), aid=" + aid + ", name=" + activityName);
		return activityName;
	}
}
