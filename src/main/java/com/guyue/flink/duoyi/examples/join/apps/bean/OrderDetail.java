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
public class OrderDetail implements Cloneable {

	private Long id;
	private Long orderId;
	private String categoryName;
	private String sku;
	private double money;
	private int amount;
	private Date createTime;
	private Date updateTime;

	// INSERT, UPDATE
	private String type;
	private int count;

	public OrderDetail() {
	}

	public OrderDetail(Long id, Long orderId, String categoryName, String sku, double money, int amount, Date createTime, Date updateTime,
					   String type) {
		this.id = id;
		this.orderId = orderId;
		this.categoryName = categoryName;
		this.sku = sku;
		this.money = money;
		this.amount = amount;
		this.createTime = createTime;
		this.updateTime = updateTime;
		this.type = type;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getOrderId() {
		return orderId;
	}

	public void setOrderId(Long orderId) {
		this.orderId = orderId;
	}

	public String getCategoryName() {
		return categoryName;
	}

	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}

	public String getSku() {
		return sku;
	}

	public void setSku(String sku) {
		this.sku = sku;
	}

	public double getMoney() {
		return money;
	}

	public void setMoney(double money) {
		this.money = money;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
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
		return "OrderDetail{" +
			"id=" + id +
			", orderId=" + orderId +
			", categoryName='" + categoryName + '\'' +
			", sku=" + sku +
			", money=" + money +
			", amount=" + amount +
			", createTime=" + createTime +
			", updateTime=" + updateTime +
			", type='" + type + '\'' +
			", count=" + count +
			'}';
	}

	@Override
	public OrderDetail clone() throws CloneNotSupportedException {
		return (OrderDetail) super.clone();
	}

	public static void main(String[] args) {

		OrderDetail orderDetailFood = new OrderDetail();
		orderDetailFood.setId(200001L);
		orderDetailFood.setAmount(1);
		orderDetailFood.setOrderId(100001L);
		orderDetailFood.setCategoryName("食品");
		orderDetailFood.setMoney(8);
		orderDetailFood.setSku("sku-food-0001");
		orderDetailFood.setCount(1);
		orderDetailFood.setCreateTime(Calendar.getInstance().getTime());
		orderDetailFood.setUpdateTime(Calendar.getInstance().getTime());
		orderDetailFood.setType("INSERT");

		System.out.println(JSON.toJSONString(orderDetailFood));

		OrderDetail orderDetailTool = new OrderDetail();
		orderDetailTool.setId(200002L);
		orderDetailTool.setAmount(1);
		orderDetailTool.setOrderId(100001L);
		orderDetailTool.setCategoryName("工具");
		orderDetailTool.setMoney(1000);
		orderDetailTool.setSku("sku-tool-0002");
		orderDetailTool.setCount(1);
		orderDetailTool.setCreateTime(Calendar.getInstance().getTime());
		orderDetailTool.setUpdateTime(Calendar.getInstance().getTime());
		orderDetailTool.setType("INSERT");
		System.out.println(JSON.toJSONString(orderDetailTool));


	}

}
