package io.mycat.plan.common.time;

public class DateTimeFormat {
//	public char time_separator; /* Separator between hour and minute */
//	public int flag; /* For future */
	public String format;

	public DateTimeFormat() {

	}

//	public DateTimeFormat(char time_separator, int flag, String format) {
//		this.time_separator = time_separator;
//		this.flag = flag;
//		this.format = format;
//	}
	public DateTimeFormat(String format) {
		this.format = format;
	}
}
