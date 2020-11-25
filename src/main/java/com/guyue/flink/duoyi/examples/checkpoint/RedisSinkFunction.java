package com.guyue.flink.duoyi.examples.checkpoint;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * @ClassName RedisSinkFunction
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-10 13:25
 */
public class RedisSinkFunction extends RichSinkFunction<Tuple3<String, String, Integer>> {


	private transient Jedis jedis;

	@Override
	public void open(Configuration configuration) throws Exception {

		super.open(configuration);

		ExecutionConfig executionConfig = getRuntimeContext().getExecutionConfig();
		ParameterTool parameterTool = (ParameterTool) executionConfig.getGlobalJobParameters();

		String redisHost = parameterTool.get("redis.host", "localhost");
		int redisPort = parameterTool.getInt("redis.port", 6379);
		jedis = new Jedis(redisHost, redisPort);
	}

	@Override
	public void close() throws Exception {
		super.close();
		jedis.close();
	}

	@Override
	public void invoke(Tuple3<String, String, Integer> tuple3, Context context) throws Exception {
		if (!jedis.isConnected()) {
			jedis.connect();
		}

		jedis.hset(tuple3.f0, tuple3.f1, String.valueOf(tuple3.f2));
	}
}
