package io.mycat.plan.common.time;

import java.math.BigDecimal;

/*
 * Structure returned by gettimeofday(2) system call,
 * and used in other calls.
 */
public class Timeval {
	public long tv_sec; /* seconds */
	public long tv_usec; /* and microseconds */

	public Timeval() {
		tv_sec = tv_usec = 0;
	}

	/**
	 * see My_decimal.cc /** Convert timeval value to my_decimal. my_decimal
	 * *timeval2my_decimal(const struct timeval *tm, my_decimal *dec)
	 * 
	 * @return
	 */
	public BigDecimal timeval2my_decimal() {
		BigDecimal intpart = BigDecimal.valueOf(tv_sec);
		BigDecimal frac = BigDecimal.valueOf(tv_usec / 1000000.0);
		return intpart.add(frac);
	}

	/**
	 * Print a timestamp with an oprional fractional part: XXXXX[.YYYYY]
	 * 
	 * @param tm
	 *            The timestamp value to print.
	 * @param OUT
	 *            to The string pointer to print at.
	 * @param dec
	 *            Precision, in the range 0..6.
	 * @return The length of the result string.
	 */
	public String my_timeval_to_str() {
		return tv_sec + "." + tv_usec;
	}
};
