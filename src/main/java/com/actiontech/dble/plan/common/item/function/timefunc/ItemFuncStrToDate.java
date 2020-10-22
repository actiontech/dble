/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.time.DateTimeFormat;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MySQLTimestampType;
import com.actiontech.dble.plan.common.time.MyTime;

import java.util.List;

public class ItemFuncStrToDate extends ItemTemporalHybridFunc {
    private MySQLTimestampType cachedTimestampType;

    /**
     * @param args
     */
    public ItemFuncStrToDate(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "str_to_date";
    }

    @Override
    public void fixLengthAndDec() {
        maybeNull = true;
        cachedFieldType = FieldTypes.MYSQL_TYPE_DATETIME;
        cachedTimestampType = MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME;
        if (args.get(1).basicConstItem()) {
            String format = args.get(1).valStr();
            if (!args.get(1).isNullValue())
                fixFromFormat(format);
        }
    }

    /**
     * Set type of datetime value (DATE/TIME/...) which will be produced
     * according to format string.
     *
     * @param format format string
     * @note We don't process day format's characters('D', 'd', 'e') because day
     * may be a member of all date/time types.
     * @note Format specifiers supported by this function should be in sync with
     * specifiers supported by extract_date_time() function.
     */
    private void fixFromFormat(String format) {
        String timePartFrms = "HISThiklrs";
        String datePartFrms = "MVUXYWabcjmvuxyw";
        boolean datePartUsed = false, timePartUsed = false, fracSecondUsed = false;
        int val = 0;
        int end = format.length();
        char[] cs = format.toCharArray();

        for (; val != end; val++) {
            if (cs[val] == '%' && val + 1 != end) {
                val++;
                if (cs[val] == 'f')
                    fracSecondUsed = timePartUsed = true;
                else if (!timePartUsed && timePartFrms.indexOf(cs[val]) >= 0)
                    timePartUsed = true;
                else if (!datePartUsed && datePartFrms.indexOf(cs[val]) >= 0)
                    datePartUsed = true;
                if (datePartUsed && fracSecondUsed) {
                    /*
                     * frac_second_used implies time_part_used, and thus we
                     * already have all types of date-time components and can
                     * end our search.
                     */
                    cachedTimestampType = MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME;
                    cachedFieldType = FieldTypes.MYSQL_TYPE_DATETIME;
                    fixLengthAndDecAndCharsetDatetime(MyTime.MAX_DATETIME_WIDTH, MyTime.DATETIME_MAX_DECIMALS);
                    return;
                }
            }
        }

        /* We don't have all three types of date-time components */
        if (fracSecondUsed) /* TIME with microseconds */ {
            cachedTimestampType = MySQLTimestampType.MYSQL_TIMESTAMP_TIME;
            cachedFieldType = FieldTypes.MYSQL_TYPE_TIME;
            fixLengthAndDecAndCharsetDatetime(MyTime.MAX_TIME_FULL_WIDTH, MyTime.DATETIME_MAX_DECIMALS);
        } else if (timePartUsed) {
            if (datePartUsed) /* DATETIME, no microseconds */ {
                cachedTimestampType = MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME;
                cachedFieldType = FieldTypes.MYSQL_TYPE_DATETIME;
                fixLengthAndDecAndCharsetDatetime(MyTime.MAX_DATETIME_WIDTH, 0);
            } else /* TIME, no microseconds */ {
                cachedTimestampType = MySQLTimestampType.MYSQL_TIMESTAMP_TIME;
                cachedFieldType = FieldTypes.MYSQL_TYPE_TIME;
                fixLengthAndDecAndCharsetDatetime(MyTime.MAX_TIME_WIDTH, 0);
            }
        } else /* DATE */ {
            cachedTimestampType = MySQLTimestampType.MYSQL_TIMESTAMP_DATE;
            cachedFieldType = FieldTypes.MYSQL_TYPE_DATE;
            fixLengthAndDecAndCharsetDatetime(MyTime.MAX_DATE_WIDTH, 0);
        }
    }

    @Override
    protected boolean valDatetime(MySQLTime ltime, long fuzzyDate) {
        DateTimeFormat dateTimeFormat = new DateTimeFormat();
        String val = args.get(0).valStr();
        String format = args.get(1).valStr();
        boolean nullDate = false;
        if (args.get(0).isNullValue() || args.get(1).isNullValue())
            nullDate = true;
        if (!nullDate) {
            nullValue = false;
            dateTimeFormat.setFormat(format);
            if (MyTime.extractDateTime(dateTimeFormat, val, ltime, cachedTimestampType, "datetime") ||
                    ((fuzzyDate & MyTime.TIME_NO_ZERO_DATE) != 0 &&
                            (ltime.getYear() == 0 || ltime.getMonth() == 0 || ltime.getDay() == 0)))
                nullDate = true;
        }
        if (!nullDate) {
            ltime.setTimeType(cachedTimestampType);
            if (cachedTimestampType == MySQLTimestampType.MYSQL_TIMESTAMP_TIME && ltime.getDay() != 0) {
                /*
                 * Day part for time type can be nonzero value and so we should
                 * add hours from day part to hour part to keep valid time
                 * value.
                 */
                ltime.setHour(ltime.getHour() + ltime.getDay() * 24);
                ltime.setDay(0);
            }
            return false;
        }

        if (val != null && (fuzzyDate & MyTime.TIME_NO_ZERO_DATE) != 0 /* warnings */) {
            LOGGER.info("str_to_date value:" + val + " is wrong value for format:" + format);
        }
        return (nullValue = true);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncStrToDate(realArgs, charsetIndex);
    }
}
