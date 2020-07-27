/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import sun.util.calendar.CalendarUtils;

import java.util.Calendar;
import java.util.Date;

/**
 * DateUtil
 *
 * @author CrazyPig
 */
public final class DateUtil {
    private DateUtil() {
    }

    public static final String DEFAULT_DATE_PATTERN = "YYYY-MM-dd HH:mm:ss";
    public static final String DATE_PATTERN_FULL = "YYYY-MM-dd HH:mm:ss.SSSSSS";
    public static final String DATE_PATTERN_ONLY_DATE = "YYYY-MM-dd";
    public static final String DEFAULT_TIME_PATTERN = "HHH:mm:ss";
    public static final String TIME_PATTERN_FULL = "HHH:mm:ss.SSSSSS";

    /**
     * parseDate
     *
     * @param dateStr
     * @return
     */
    public static Date parseDate(String dateStr) {
        return parseDate(dateStr, DEFAULT_DATE_PATTERN);
    }

    /**
     * parseDate
     *
     * @param dateStr
     * @param datePattern
     * @return
     */
    public static Date parseDate(String dateStr, String datePattern) {
        DateTime dt = DateTimeFormat.forPattern(datePattern).parseDateTime(dateStr);
        return dt.toDate();
    }

    /**
     * getYear
     *
     * @param date
     * @return
     */
    public static int getYear(Date date) {
        DateTime dt = new DateTime(date);
        return dt.getYear();
    }

    /**
     * getMonth
     *
     * @param date
     * @return
     */
    public static int getMonth(Date date) {
        DateTime dt = new DateTime(date);
        return dt.getMonthOfYear();
    }

    /**
     * getDay
     *
     * @param date
     * @return
     */
    public static int getDay(Date date) {
        DateTime dt = new DateTime(date);
        return dt.getDayOfMonth();
    }

    /**
     * getHour
     *
     * @param date
     * @return
     */
    public static int getHour(Date date) {
        DateTime dt = new DateTime(date);
        return dt.getHourOfDay();
    }

    /**
     * getMinute
     *
     * @param date
     * @return
     */
    public static int getMinute(Date date) {
        DateTime dt = new DateTime(date);
        return dt.getMinuteOfHour();
    }

    /**
     * getSecond
     *
     * @param date
     * @return
     */
    public static int getSecond(Date date) {
        DateTime dt = new DateTime(date);
        return dt.getSecondOfMinute();
    }

    /**
     * getMicroSecond
     *
     * @param date
     * @return
     */
    public static int getMicroSecond(Date date) {
        DateTime dt = new DateTime(date);
        return dt.getMillisOfSecond();
    }

    /**
     * Get the number of days between two times
     *
     * @param cal1
     * @param cal2
     * @return
     */
    public static long diffDays(Calendar cal1, Calendar cal2) {
        boolean negativeFlag = false;
        if (cal1.after(cal2)) {
            Calendar oldCal1 = cal1;
            cal1 = cal2;
            cal2 = oldCal1;
            negativeFlag = true;
        }
        int day1 = cal1.get(Calendar.DAY_OF_YEAR);
        int day2 = cal2.get(Calendar.DAY_OF_YEAR);
        int year1 = cal1.get(Calendar.YEAR);
        int year2 = cal2.get(Calendar.YEAR);
        int diffDays;
        if (year1 != year2) {
            int timeDistance = 0;
            for (int i = year1; i < year2; i++) {
                if (i == year1) {
                    timeDistance += (CalendarUtils.isGregorianLeapYear(year1) ? 366 : 365) - day1;
                } else if (CalendarUtils.isGregorianLeapYear(i)) {
                    timeDistance += 366;
                } else {
                    timeDistance += 365;
                }
            }
            diffDays = timeDistance + day2;
        } else {
            diffDays = day2 - day1;
        }
        return negativeFlag ? diffDays : -diffDays;
    }

}
