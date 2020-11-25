package com.guyue.flink.duoyi.examples.dimension.function;

import com.guyue.flink.duoyi.examples.dimension.bean.ActivityBean;
import java.io.IOException;
import java.net.URI;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * @ClassName DataToActivityBeanMySqlFunction
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-06 20:20
 */
public class DataToActivityBeanGaoDeFunction extends RichMapFunction<String, ActivityBean> {

	private String key = "guyue_app";
	private String value = "bfdda4dc93786349fa30a2d6c0c8da71";
	private String url = "https://restapi.amap.com/v3/geocode/regeo?key=" + value;

	private HttpClient httpClient = null;

	@Override
	public void close() throws Exception {
		super.close();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
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
		String jingdu = lineArr[4];
		String weidu = lineArr[5];

		//创建HttpClinet
		CloseableHttpClient httpClient = HttpClients.createDefault();
		//拼接访问url 进行
		URI uri = new URI(url);
		//拼接搜索内容 ?wd=httpclinet
		URIBuilder uriBuilder = new URIBuilder(uri);
		uriBuilder.setParameter("location", jingdu + "," + weidu);
		URI uriParma = uriBuilder.build();

		System.out.println(" ascii-string = " + uriParma.toASCIIString());
		System.out.println(" string = " + uriParma.toString());

		HttpGet httpGet = new HttpGet(uriParma);
		CloseableHttpResponse response = null;
		String province = null;

		try {
			System.out.println();
			response = httpClient.execute(httpGet);
			int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode == 200) {
				province = EntityUtils.toString(response.getEntity(), "UTF-8");
			}
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			response.close();
		}

		return ActivityBean.of(uid, aid, activityName, time, eventType, province);
	}
}
