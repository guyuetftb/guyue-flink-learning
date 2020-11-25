package com.guyue.examples;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.shaded.org.joda.time.IllegalInstantException;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @ClassName TestAA
 * @Description TOOD
 * @Author lipeng
 * @Date 2019/6/19 18:11
 */
public class TestAA {

	private final static Logger LOG = LoggerFactory.getLogger(TestAA.class);

	public static void main(String[] args)
		throws ClassNotFoundException, InvocationTargetException, MalformedURLException, IllegalAccessException, NoSuchMethodException {
		// init data
		String jarPath = "/data/cloud-lipeng02/rt_alg_home/guyue-flink.jar";
		// jarPath = "/Users/lipeng/workspace_guyue/guyue-parents/guyue-flink/target/guyue-flink.jar";

		UDFPOJO udfPojo = new UDFPOJO();
		udfPojo.setJarPath(jarPath);

		List<String> methodsList = new ArrayList<String>();
		methodsList.add("my_hash_code");
		udfPojo.setMethodName(methodsList);
		List<String> classpathList = new ArrayList<String>();
		classpathList.add("com.guyue.flink.udf.MYHashCode");
		udfPojo.setClassPath(classpathList);

		List<UDFPOJO> jarFilesList = new ArrayList<UDFPOJO>();
		jarFilesList.add(udfPojo);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		String topic = "rt_alg_search_feature_etl";
		Properties consumerPro = new Properties();
		consumerPro.setProperty("max.poll.records", "1");
		consumerPro.setProperty("bootstrap.servers", "10.3.32.47:9092,10.3.32.8:9092,10.3.32.4:9092");
		consumerPro.setProperty("group.id", "rtalgschfturetl_udf_001");

		Properties producerPro = new Properties();
		producerPro.setProperty("bootstrap.servers", "10.3.32.47:9092,10.3.32.8:9092,10.3.32.4:9092");
		producerPro.setProperty("enable.auto.commit", "false");
		producerPro.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerPro.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerPro.setProperty("compression.type", "lz4");

		FlinkKafkaConsumer kafkaConsumer010 = new FlinkKafkaConsumer(topic, new SimpleStringSchema(), consumerPro);
		SingleOutputStreamOperator<Row> singleOper = env.addSource(kafkaConsumer010).map(new MapFunction() {

			@Override
			public Row map(Object value) throws Exception {
				JSONObject jsonObject = JSONObject.parseObject(value.toString());
				Row row = new Row(4);
				row.setField(0, jsonObject.getString("user_id"));
				row.setField(1, jsonObject.getString("event"));
				row.setField(2, jsonObject.getString("label"));
				row.setField(3, jsonObject.getString("business"));
				return row;
			}
		});

		String tableName = "test_rt_alg_search_feature_etl";
		String[] fields = new String[]{"user_id", "event", "label", "business", "hash_code"};
		TypeInformation[] fieldsType = new TypeInformation[]{Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()};

		tableEnv.registerDataStream(tableName, singleOper, "user_id,event,label,business,hash_code");
		registerUDF(tableEnv, jarFilesList);
		Table featureEtlResultTab = tableEnv
			.sqlQuery("select user_id, event, label, business, my_hash_code(business) as hash_code from " + tableName);

		FlinkKafkaProducer kafkaProducer010 = new FlinkKafkaProducer("10.3.32.47:9092,10.3.32.8:9092,10.3.32.4:9092", "test", new SimpleStringSchema());
		DataStream da = tableEnv.toRetractStream(featureEtlResultTab, Types.ROW(fields, fieldsType));
		System.out.println(System.getProperty("java.ext.dirs"));
		da.addSink(kafkaProducer010);

		try {
			env.execute("UDF-Testing");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void registerUDF(StreamTableEnvironment tableEnv,
								   List<UDFPOJO> jarFilesList)
		throws ClassNotFoundException, MalformedURLException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

		ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
		MyClassLoader myURLClassLoader = new MyClassLoader(new URL[]{}, parentClassLoader);

		Thread.currentThread().setContextClassLoader(myURLClassLoader);

		System.out.println("---------------------> myURLClassLoader = " + myURLClassLoader.getClass());
		System.out.println("---------------------> parentClassLoader = " + parentClassLoader.getClass());
		URLClassLoader contextClassLoaderParent = (URLClassLoader) Thread.currentThread().getContextClassLoader().getParent();
		System.out.println("---------------------> contextClassLoaderParent = " + contextClassLoaderParent.getClass());
		URLClassLoader contextClassLoaderParentParent = (URLClassLoader) Thread.currentThread().getContextClassLoader().getParent().getParent();
		System.out.println("---------------------> contextClassLoaderParentParent = " + contextClassLoaderParentParent.getClass());

		/** register hive udf */
		registerHiveUDF(tableEnv, myURLClassLoader);

		/** validate jar file */
		validateJarFile(jarFilesList);

		/** register table udf */
		for (UDFPOJO udfPojo : jarFilesList) {
			// 这里不能直接创建URL, 必须是先创建文件,再转URI,再转URL
			urlClassLoaderAddURL(myURLClassLoader, new File(udfPojo.getJarPath()).toURI().toURL());
			List<String> classpaths = udfPojo.getClassPath();
			List<String> methodNames = udfPojo.getClassPath();

			for (int index = 0; index < classpaths.size(); index++) {
				String classpath = methodNames.get(index);
				String methodName = methodNames.get(index);

				// load class
				myURLClassLoader.loadClass(classpath);

				// register function
				registerUDFInternel(tableEnv, myURLClassLoader, methodName, classpath);
			}
		}
	}

	public static void registerHiveUDF(StreamTableEnvironment tableEnv,
									   ClassLoader classLoader) {

	}

	public static void registerUDFInternel(StreamTableEnvironment tableEnv,
										   ClassLoader classLoader,
										   String methodName,
										   String classpath) {

		try {
			UserDefinedFunction functionClazz = Class.forName(classpath, false, classLoader)
				.asSubclass(UserDefinedFunction.class)
				.newInstance();

			if (functionClazz instanceof ScalarFunction) {
				tableEnv.registerFunction(methodName, (ScalarFunction) functionClazz);
			} else if (functionClazz instanceof TableFunction) {
				tableEnv.registerFunction(methodName, (TableFunction) functionClazz);
			} else if (functionClazz instanceof AggregateFunction) {
				tableEnv.registerFunction(methodName, (AggregateFunction) functionClazz);
			}

			LOG.info("register table function:{} success.", classpath);
		} catch (Exception e) {
			throw new RuntimeException(" Register Table UDF exception:", e);
		}
	}

	public static void validateJarFile(List<UDFPOJO> udfEntityList) {
		if (null == udfEntityList || udfEntityList.isEmpty()) {
			throw new IllegalInstantException(" The jar file is null or empty. ");
		}

		for (UDFPOJO udfPojo : udfEntityList) {
			validateJarFile(udfPojo);
		}
	}

	public static void validateJarFile(UDFPOJO udfPojo) {
		if (null == udfPojo.getMethodName() || udfPojo.getMethodName().isEmpty()) {
			throw new IllegalInstantException(" The method name is null or empty.");
		}

		if (null == udfPojo.getClassPath() || udfPojo.getClassPath().isEmpty()) {
			throw new IllegalInstantException(" The class path name is null or empty.");
		}

		if (StringUtils.isEmpty(udfPojo.getJarPath())) {
			throw new IllegalInstantException(" The file is null or empty.");
		}

		if (!udfPojo.getJarPath().endsWith(".jar")) {
			throw new IllegalInstantException(" Jar file format is incorrect. ");
		}
	}

	private static void urlClassLoaderAddURL(URLClassLoader urlClassLoader,
											 URL url) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Method method = urlClassLoader.getClass().getMethod("addURL", URL.class);
		method.setAccessible(true);
		method.invoke(urlClassLoader, url);
	}
}
