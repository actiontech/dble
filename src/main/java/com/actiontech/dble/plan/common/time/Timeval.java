/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.time;

import java.math.BigDecimal;

/*
 * Structure returned by gettimeofday(2) system call,
 * and used in other calls.
 */
public class Timeval {
    private long tvSec; /* seconds */
    private long tvUsec; /* and microseconds */

    public Timeval() {
        tvSec = tvUsec = 0;
    }

    /**
     * see My_decimal.cc /** Convert timeval value to my_decimal. my_decimal
     * *timeval2my_decimal(const struct timeval *tm, my_decimal *dec)
     *
     * @return
     */
    public BigDecimal timeval2MyDecimal() {
        BigDecimal intpart = BigDecimal.valueOf(tvSec);
        BigDecimal frac = BigDecimal.valueOf(tvUsec / 1000000.0);
        return intpart.add(frac);
    }

    /**
     * Print a timestamp with an oprional fractional part: XXXXX[.YYYYY]
     *
     * @param tm  The timestamp value to print.
     * @param OUT to The string pointer to print at.
     * @param dec Precision, in the range 0..6.
     * @return The length of the result string.
     */
    public String myTimevalToStr() {
        return tvSec + "." + tvUsec;
    }

    public long getTvSec() {
        return tvSec;
    }

    public void setTvSec(long tvSec) {
        this.tvSec = tvSec;
    }

    public long getTvUsec() {
        return tvUsec;
    }

    public void setTvUsec(long tvUsec) {
        this.tvUsec = tvUsec;
    }
}
