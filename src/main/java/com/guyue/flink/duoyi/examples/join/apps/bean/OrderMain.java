package com.guyue.flink.duoyi.examples.join.apps.bean;

import com.alibaba.fastjson.JSON;
import java.util.Calendar;
import java.util.Date;

/**
 * @ClassName OrderMain
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-10 22:18
 */
public class OrderMain implements Cloneable {

	private Long orderId;
	private Date createTime;
	private double totalMoney;
	private int status;
	private Date updateTime;
	private String province;
	private String city;

	// INSERT, UPDATE
	private String type;
	private int count;

	public OrderMain(Long orderId, Date createTime, double totalMoney, int status, Date updateTime, String province, String city, String type) {
		this.orderId = orderId;
		this.createTime = createTime;
		this.totalMoney = totalMoney;
		this.status = status;
		this.updateTime = updateTime;
		this.province = province;
		this.city = city;
		this.type = type;
	}

	public OrderMain() {
	}

	public Long getOrderId() {
		return orderId;
	}

	public void setOrderId(Long orderId) {
		this.orderId = orderId;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public double getTotalMoney() {
		return totalMoney;
	}

	public void setTotalMoney(double totalMoney) {
		this.totalMoney = totalMoney;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "OrderMain{" +
			"orderId=" + orderId +
			", createTime=" + createTime +
			", totalMoney=" + totalMoney +
			", status=" + status +
			", updateTime=" + updateTime +
			", province='" + province + '\'' +
			", city='" + city + '\'' +
			", type='" + type + '\'' +
			", count=" + count +
			'}';
	}

	@Override
	public OrderMain clone() throws CloneNotSupportedException {
		return (OrderMain) super.clone();
	}

	public static void main(String[] args) {

		OrderMain orderMain = new OrderMain();
		orderMain.setOrderId(100001L);
		orderMain.setCity("北京");
		orderMain.setProvince("北京");
		orderMain.setStatus(1);
		orderMain.setCount(1);
		orderMain.setCreateTime(Calendar.getInstance().getTime());
		orderMain.setUpdateTime(Calendar.getInstance().getTime());
		orderMain.setTotalMoney(1008.5);
		orderMain.setType("INSERT");

		System.out.println(JSON.toJSONString(orderMain));

	}
}
