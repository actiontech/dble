package io.mycat.plan.common.time;

public class INTERVAL {
	public long year, month, day, hour;
	public long minute, second, second_part;
	public boolean neg;

	public INTERVAL() {
		year = month = day = hour = minute = second = second_part = 0;
		neg = false;
	}
}
