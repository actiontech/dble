/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.text.ParseException;
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
     * @throws ParseException
     */
    public static Date parseDate(String dateStr) throws ParseException {
        return parseDate(dateStr, DEFAULT_DATE_PATTERN);
    }

    /**
     * parseDate
     *
     * @param dateStr
     * @param datePattern
     * @return
     * @throws ParseException
     */
    public static Date parseDate(String dateStr, String datePattern) throws ParseException {
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

}
