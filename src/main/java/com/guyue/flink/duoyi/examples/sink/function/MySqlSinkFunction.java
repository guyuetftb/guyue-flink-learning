package com.guyue.flink.duoyi.examples.sink.function;

import com.guyue.flink.duoyi.examples.dimension.bean.ActivityBean;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @ClassName MySqlSinkFunction
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-06 22:56
 */
public class MySqlSinkFunction extends RichSinkFunction<ActivityBean> {

	private String url = "jdbc:mysql://127.0.0.1:3306/test_master?characterEncoding=utf8&useSSL=false&allowMultiQueries=true";
	private String user = "root";
	private String password = "123456abc";
	private String insertUpdateSql = "insert into t_activities_count (aid, event_type, `count`) values (?, ?, ?) ON DUPLICATE KEY UPDATE `count` = `count` + ?";

	private Connection connection = null;

	@Override
	public void close() throws Exception {
		super.close();
		connection.close();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		connection = DriverManager.getConnection(url, user, password);
	}

	@Override
	public void invoke(ActivityBean activityBean, Context context) throws Exception {
		PreparedStatement statement = connection.prepareStatement(insertUpdateSql);
		statement.setString(1, activityBean.aid);
		statement.setInt(2, activityBean.eventType);
		statement.setInt(3, activityBean.count);
		statement.setInt(4, activityBean.count);
		int resultNum = statement.executeUpdate();

		System.out.println(Thread.currentThread().getName() + ", " + activityBean);
		System.out.println(Thread.currentThread().getName() + ", update-number = " + resultNum);
		statement.close();
	}
}
