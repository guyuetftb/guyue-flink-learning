package com.guyue.flink.duoyi.examples.dimension.function;

import com.guyue.flink.duoyi.examples.dimension.bean.ActivityBean;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * @ClassName DataToActivityBeanMySqlFunction
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-06 20:20
 */
public class DataToActivityBeanMySqlFunction extends RichMapFunction<String, ActivityBean> {

	private String url = "jdbc:mysql://127.0.0.1:3306/test_master?characterEncoding=utf8&useSSL=false&allowMultiQueries=true";
	private String user = "root";
	private String password = "123456abc";

	private Connection connection = null;
	private PreparedStatement statement = null;

	@Override
	public void close() throws Exception {
		super.close();
		statement.close();
		connection.close();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		connection = DriverManager.getConnection(url, user, password);
		statement = connection.prepareStatement("select name from t_activities where id = ?");
	}

	@Override
	public ActivityBean map(String line) throws Exception {
		// u001,A1,2019-09-02 10:10:11,1,北京市
		String[] lineArr = line.split(",");

		String activityName = null;
		String uid = lineArr[0];
		String aid = lineArr[1];
		String time = lineArr[2];
		int eventType = Integer.parseInt(lineArr[3]);
		String province = lineArr[4];

		statement.setString(1, aid);
		ResultSet rs = statement.executeQuery();
		while (rs.next()) {
			activityName = rs.getString("name");
		}

		return ActivityBean.of(uid, aid, activityName, time, eventType, province);
	}
}
