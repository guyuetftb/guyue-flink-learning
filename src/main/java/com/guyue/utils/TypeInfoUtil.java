package com.guyue.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TypeInfoUtil {

	private static Logger LOG = LoggerFactory.getLogger(TypeInfoUtil.class);

	/**
	 * 字符串类型对应的flink类型信息map
	 */
	private static Map<String, TypeInformation> columnType2TypeInformation = new HashMap<>(6);

	static {
		columnType2TypeInformation.put("string", Types.STRING);
		columnType2TypeInformation.put("integer", Types.INT);
		columnType2TypeInformation.put("double", Types.DOUBLE);
		columnType2TypeInformation.put("bigInt", Types.LONG);
		columnType2TypeInformation.put("float", Types.FLOAT);
		columnType2TypeInformation.put("decimal", Types.BIG_DEC);
	}

	/**
	 *
	 */
	public static RowTypeInfo getRowTypeInfo(final String tableAsName) throws SQLException {

		System.out.println("getRowTypeInfo 获取： " + tableAsName + " 元数据");
		//2.获得数据库链接
		Connection conn = JdbcUtil.getConn();
		//3.通过数据库的连接操作数据库，实现增删改查（使用Statement类）
		Statement st = conn.createStatement();

		HashMap<Integer, String> orderNum2Column = new HashMap<>(16);
		HashMap<String, String> column2type = new HashMap<>(16);
		String columnName;
		String orderNum;
		String columnType;
		//获取所有字段
		ResultSet tableInfoRs = st.executeQuery("select * from rtdp_columns_info where table_name='" + tableAsName + "'");
		while (tableInfoRs.next()) {
			columnName = tableInfoRs.getString("column_name");
			orderNum = tableInfoRs.getString("order_num");
			columnType = tableInfoRs.getString("column_type");
			orderNum2Column.put(Integer.valueOf(orderNum), columnName);
			column2type.put(columnName, columnType);
		}

		TypeInformation[] typeInformations = new TypeInformation[orderNum2Column.size()];
		orderNum2Column.forEach((k, v) -> {
			typeInformations[k] = columnType2TypeInformation.getOrDefault(column2type.get(v.toLowerCase()),
				Types.STRING);
		});

		//关闭连接
		tableInfoRs.close();
		st.close();

		RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInformations, orderNum2Column.values().toArray(new String[0]));
		System.out.println("RowTypeInfo: " + rowTypeInfo);
		return rowTypeInfo;
	}

	/**
	 * sql中如果某个udf或者函数没有用`as field name`,那么需要给它一个名字，不然最终生成的数据json字段名是`EXPR。。。`
	 */
	public static String[] fillFields(String[] finalColumnNames) {
		for (int i = 0; i < finalColumnNames.length; i++) {
			if (finalColumnNames[i].contains("EXPR")) {
				//不可以用`f`
				finalColumnNames[i] = "cf" + i;
			}
		}
		System.out.println("final column:  " + Arrays.toString(finalColumnNames));
		return finalColumnNames;
	}

	public static void main(String[] args) {
		HashMap<Integer, String> orderNum2Column = new HashMap<>();
		orderNum2Column.put(0, "a");
		orderNum2Column.put(3, "d");
		orderNum2Column.put(11, "----");
		orderNum2Column.put(2, "c");
		orderNum2Column.put(1, "b");
		System.out.println(orderNum2Column);
	}
}

