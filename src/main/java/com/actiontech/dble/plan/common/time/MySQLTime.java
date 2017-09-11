/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.time;

import java.io.Serializable;
import java.util.Calendar;

public class MySQLTime implements Serializable {
    private static final long serialVersionUID = 8021983316690707464L;
    private long year;
    private long month;
    private long day;
    private long hour;
    private long minute;
    private long second;
    private long secondPart;
    private boolean neg;
    private MySQLTimestampType timeType;

    public void setZeroTime(MySQLTimestampType type) {
        year = month = day = hour = minute = second = secondPart = 0;
        neg = false;
        timeType = type;
    }

    public boolean isNonZeroDate() {
        return year != 0 || month != 0 || day != 0;
    }

    public boolean isNonZeroTime() {
        return hour != 0 || minute != 0 || second != 0 || secondPart != 0;
    }

    public java.util.Calendar toCalendar() {
        java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.set((int) year, (int) month, (int) day, (int) hour, (int) minute, (int) second);
        cal.set(java.util.Calendar.MILLISECOND, (int) secondPart / 1000);
        return cal;
    }

    public void setMaxHhmmss() {
        hour = MyTime.TIME_MAX_HOUR;
        minute = MyTime.TIME_MAX_MINUTE;
        second = MyTime.TIME_MAX_SECOND;
    }

    public void setCal(java.util.Calendar cal) {
        year = cal.get(Calendar.YEAR);
        month = cal.get(Calendar.MONTH) + 1;
        day = cal.get(Calendar.DAY_OF_MONTH);
        hour = cal.get(Calendar.HOUR_OF_DAY);
        minute = cal.get(Calendar.MINUTE);
        second = cal.get(Calendar.SECOND);
        secondPart = (cal.getTimeInMillis() % 1000) * 1000;
    }

    public int getCompareResult(MySQLTime other) {
        if (other == null)
            return 1;
        long lt1 = MyTime.timeToLonglongDatetimePacked(this);
        long lt2 = MyTime.timeToLonglongDatetimePacked(other);
        long cmp = lt1 - lt2;
        return cmp == 0 ? 0 : (lt1 > lt2 ? 1 : -1);
    }

    public long getYear() {
        return year;
    }

    public void setYear(long year) {
        this.year = year;
    }

    public long getMonth() {
        return month;
    }

    public void setMonth(long month) {
        this.month = month;
    }

    public long getDay() {
        return day;
    }

    public void setDay(long day) {
        this.day = day;
    }

    public long getHour() {
        return hour;
    }

    public void setHour(long hour) {
        this.hour = hour;
    }

    public long getMinute() {
        return minute;
    }

    public void setMinute(long minute) {
        this.minute = minute;
    }

    public long getSecond() {
        return second;
    }

    public void setSecond(long second) {
        this.second = second;
    }

    public long getSecondPart() {
        return secondPart;
    }

    public void setSecondPart(long secondPart) {
        this.secondPart = secondPart;
    }

    /**
     * < microseconds
     */
    public boolean isNeg() {
        return neg;
    }

    public void setNeg(boolean neg) {
        this.neg = neg;
    }

    public MySQLTimestampType getTimeType() {
        return timeType;
    }

    public void setTimeType(MySQLTimestampType timeType) {
        this.timeType = timeType;
    }
}
