package io.mycat.plan.common.time;

import java.io.Serializable;
import java.util.Calendar;

public class MySQLTime  implements Serializable {
	private static final long serialVersionUID = 8021983316690707464L;
	public long year, month, day, hour, minute, second;
	public long second_part;
	/** < microseconds */
	public boolean neg;
	public MySQLTimestampType time_type;

	public void set_zero_time(MySQLTimestampType type) {
		year = month = day = hour = minute = second = second_part = 0;
		neg = false;
		time_type = type;
	}

	public boolean isNonZeroDate() {
		return year != 0 || month != 0 || day != 0;
	}

	public boolean isNonZeroTime() {
		return hour != 0 || minute != 0 || second != 0 || second_part != 0;
	}

	public java.util.Calendar toCalendar() {
		java.util.Calendar cal = java.util.Calendar.getInstance();
		cal.set((int) year, (int) month, (int) day, (int) hour, (int) minute, (int) second);
		cal.set(java.util.Calendar.MILLISECOND, (int) second_part / 1000);
		return cal;
	}

	public void set_max_hhmmss() {
		hour = MyTime.TIME_MAX_HOUR;
		minute = MyTime.TIME_MAX_MINUTE;
		second = MyTime.TIME_MAX_SECOND;
	}

	public void setCal(java.util.Calendar cal) {
		year = cal.get(Calendar.YEAR);
		month = cal.get(Calendar.MONTH);
		day = cal.get(Calendar.DAY_OF_MONTH);
		hour = cal.get(Calendar.HOUR_OF_DAY);
		minute = cal.get(Calendar.MINUTE);
		second = cal.get(Calendar.SECOND);
		second_part = (cal.getTimeInMillis() % 1000) * 1000;
	}

	public int compareTo(MySQLTime other) {
		if (other == null)
			return 1;
		long lt1 = MyTime.TIME_to_longlong_datetime_packed(this);
		long lt2 = MyTime.TIME_to_longlong_datetime_packed(other);
		long cmp = lt1 - lt2;
		return cmp == 0 ? 0 : (lt1 > lt2 ? 1 : -1);
	}

}
