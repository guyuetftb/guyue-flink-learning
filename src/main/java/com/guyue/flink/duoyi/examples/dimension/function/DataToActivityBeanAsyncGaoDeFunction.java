package com.guyue.flink.duoyi.examples.dimension.function;

import com.guyue.flink.duoyi.examples.dimension.bean.ActivityBean;
import java.net.URI;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
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
public class DataToActivityBeanAsyncGaoDeFunction extends RichAsyncFunction<String, ActivityBean> {

	private String key = "guyue_app";
	private String value = "bfdda4dc93786349fa30a2d6c0c8da71";
	private String url = "https://restapi.amap.com/v3/geocode/regeo?key=" + value;

	private transient CloseableHttpAsyncClient asyncHttpClient = null;


	@Override
	public void open(Configuration parameters) throws Exception {
		System.out.println(" timestamp = " + System.currentTimeMillis() + ", open() ");

		RequestConfig requestConfig = RequestConfig.custom()
			.setSocketTimeout(3000)        // 请求超时时间
			.setConnectTimeout(3000)    // 创建连接超时时间
			.build();
		asyncHttpClient = HttpAsyncClients.custom()
			.setMaxConnTotal(20)    // 最大20个请求
			.setDefaultRequestConfig(requestConfig)
			.build();
		asyncHttpClient.start();

		super.open(parameters);
	}

	@Override
	public void close() throws Exception {
		super.close();
		asyncHttpClient.close();
		System.out.println(" timestamp = " + System.currentTimeMillis() + ", close() ");
	}

	@Override
	public void timeout(String input, ResultFuture<ActivityBean> resultFuture) throws Exception {
		System.out.println(" timestamp = " + System.currentTimeMillis() + ", input = " + input + ", timeout()");
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
		String jingdu = lineArr[4];
		String weidu = lineArr[5];

		//创建HttpClinet

		//拼接访问url 进行
		URI uri = new URI(url);
		//拼接搜索内容 ?wd=httpclinet
		URIBuilder uriBuilder = new URIBuilder(uri);
		uriBuilder.setParameter("location", jingdu + "," + weidu);
		URI uriParma = uriBuilder.build();
		HttpGet httpGet = new HttpGet(uriParma);

		System.out.println(" ascii-string = " + uriParma.toASCIIString());
		System.out.println(" string = " + uriParma.toString());

		Future<HttpResponse> httpResponseFuture = asyncHttpClient.execute(httpGet, new FutureCallback<HttpResponse>() {
			@Override
			public void completed(HttpResponse httpResponse) {
				System.out.println("time = " + System.currentTimeMillis() + ", FutureCallback ---> completed");
			}

			@Override
			public void failed(Exception e) {
				System.out.println("time = " + System.currentTimeMillis() + ", FutureCallback ---> failed");
			}

			@Override
			public void cancelled() {
				System.out.println("time = " + System.currentTimeMillis() + ", FutureCallback ---> cancelled");
			}
		});

		CompletableFuture.supplyAsync(new Supplier<String>() {

			@Override
			public String get() {
				System.out.println(" timestamp = " + System.currentTimeMillis() + ", get()");
				String province = null;
				try {
					HttpResponse httpResponse = httpResponseFuture.get();
					if (httpResponse.getStatusLine().getStatusCode() == 200) {
						province = EntityUtils.toString(httpResponse.getEntity());
					}
					return province;
				} catch (Exception e) {
					return "ERROR-" + province;
				}

			}
		}).thenAccept(new Consumer<String>() {
			@Override
			public void accept(String province) {
				System.out.println(" timestamp = " + System.currentTimeMillis() + ", Consumer.accept()");
				resultFuture.complete(Collections.singleton(ActivityBean.of(uid, aid, activityName, time, eventType, province)));
			}
		});
	}
}
