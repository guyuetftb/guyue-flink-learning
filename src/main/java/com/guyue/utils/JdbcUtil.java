package com.guyue.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcUtil {

	private static final Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static String osName = System.getProperties().getProperty("os.name");
	private static String DB_URL;
	private static String USER;
	private static String PASS;

	static {
		/**
		 * url: jdbc:mysql://10.3.5.136:3306/streamcube?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true
		 *       username: streamcube_rw
		 *       password: streamcube3AHEd5OV
		 */
		if ("Linux".equals(osName)) {
			DB_URL = "jdbc:mysql://10.3.5.136:3306/streamcube?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true";
			USER = "streamcube_rw";
			PASS = "streamcube3AHEd5OV";
		} else {
			DB_URL = "jdbc:mysql://10.2.40.10:3306/bifrost";
			USER = "root";
			PASS = "cfiKRecV1tKW!";
		}
	}

	public static Connection getConn() {
		Connection conn = null;
		try {
			// 注册 JDBC 驱动
			Class.forName(JDBC_DRIVER);
			// 打开链接
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
		} catch (Exception e) {
			// 处理 Class.forName 错误
			e.printStackTrace();
			LOG.error("连接" + DB_URL + "数据库错误: " + e.getMessage());
		}
		LOG.info("连接数据库 conn..." + conn);
		try {
			LOG.warn("STAT: " + conn.createStatement().toString());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static Connection getMetaConn(String ip, String user, String pass) {
		String url = "jdbc:mysql://" + ip + "/streamcube?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true";
		Connection conn = null;
		try {
			// 注册 JDBC 驱动
			Class.forName(JDBC_DRIVER);
			// 打开链接
			conn = DriverManager.getConnection(url, user, pass);
		} catch (Exception e) {
			// 处理 Class.forName 错误
			e.printStackTrace();
			LOG.error("连接" + url + "数据库错误: " + e.getMessage());
		}
		LOG.info("连接数据库 conn..." + conn);
		try {
			LOG.warn("STAT: " + conn.createStatement().toString());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}


	public static void main(String[] args) {
		Connection conn = null;
		Statement stmt = null;
		try {
			// 注册 JDBC 驱动
			Class.forName("com.mysql.jdbc.Driver");

			// 打开链接
			System.out.println("连接数据库...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			// 执行查询
			System.out.println(" 实例化Statement对象...");
			stmt = conn.createStatement();
			String sql;
			sql = "SELECT * FROM rt_job";
			ResultSet rs = stmt.executeQuery(sql);
			// 展开结果集数据库
			while (rs.next()) {
				// 通过字段检索
				int id = rs.getInt("id");
				// 输出数据
				System.out.print("ID: " + id);
				System.out.print("\n");
			}
			// 完成后关闭
			rs.close();
			stmt.close();
			conn.close();
		} catch (SQLException se) {
			// 处理 JDBC 错误
			se.printStackTrace();
		} catch (Exception e) {
			// 处理 Class.forName 错误
			e.printStackTrace();
		} finally {
			// 关闭资源
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException se2) {
			}// 什么都不做
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		System.out.println("Goodbye!");
	}
}
