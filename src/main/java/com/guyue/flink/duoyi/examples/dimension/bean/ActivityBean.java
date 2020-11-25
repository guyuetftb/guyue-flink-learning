package com.guyue.flink.duoyi.examples.dimension.bean;

/**
 * @ClassName ActivityBean
 * @Description TOOD
 * @Author lipeng
 * @Date 2020-03-06 20:17
 */
public class ActivityBean {

	// u001,A1,2019-09-02 10:10:11,1,北京市
	public String uid;
	public String aid;
	public String activityName;
	public String time;
	public int eventType;
	public String province;
	public int count = 1;

	public ActivityBean() {

	}

	public ActivityBean(String uid, String aid, String activityName, String time, int eventType, String province) {
		this.uid = uid;
		this.aid = aid;
		this.activityName = activityName;
		this.time = time;
		this.eventType = eventType;
		this.province = province;
	}

	@Override
	public String toString() {
		return "ActivityBean{" +
			"uid='" + uid + '\'' +
			", aid='" + aid + '\'' +
			", activityName='" + activityName + '\'' +
			", time='" + time + '\'' +
			", eventType=" + eventType +
			", province='" + province + '\'' +
			", count=" + count +
			'}';
	}

	public static ActivityBean of(String uid, String aid, String activityName, String time, int eventType, String province) {
		return new ActivityBean(uid, aid, activityName, time, eventType, province);
	}
}
