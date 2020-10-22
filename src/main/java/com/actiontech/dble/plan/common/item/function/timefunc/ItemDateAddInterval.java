/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

/**
 *
 */
package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.actiontech.dble.plan.common.time.*;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntervalExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntervalUnit;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import java.util.ArrayList;
import java.util.List;


public class ItemDateAddInterval extends ItemTemporalHybridFunc {
    StringPtr strValue = new StringPtr("");
    private SQLIntervalUnit intType;
    private boolean dateSubInterval;

    public ItemDateAddInterval(Item a, Item b, SQLIntervalUnit type, boolean neg, int charsetIndex) {
        super(new ArrayList<Item>(), charsetIndex);
        args.add(a);
        args.add(b);
        this.intType = type;
        this.dateSubInterval = neg;
    }

    @Override
    public final String funcName() {
        return "DATE_ADD";
    }

    @Override
    public void fixLengthAndDec() {
        FieldTypes arg0FieldType;

        maybeNull = true;

        /*
         * The field type for the result of an Item_date function is defined as
         * follows:
         *
         * - If first arg is a MYSQL_TYPE_DATETIME result is MYSQL_TYPE_DATETIME
         * - If first arg is a MYSQL_TYPE_DATE and the interval type uses hours,
         * minutes or seconds then type is MYSQL_TYPE_DATETIME. - Otherwise the
         * result is MYSQL_TYPE_STRING (This is because you can't know if the
         * string contains a DATE, MYSQL_TIME or DATETIME argument)
         */
        arg0FieldType = args.get(0).fieldType();
        int intervalDec = 0;
        if (intType == SQLIntervalUnit.MICROSECOND || intType == SQLIntervalUnit.DAY_MICROSECOND ||
                intType == SQLIntervalUnit.HOUR_MICROSECOND || intType == SQLIntervalUnit.MINUTE_MICROSECOND ||
                intType == SQLIntervalUnit.SECOND_MICROSECOND)
            intervalDec = MyTime.DATETIME_MAX_DECIMALS;
        else if (intType == SQLIntervalUnit.SECOND && args.get(1).getDecimals() > 0)
            intervalDec = Math.min(args.get(1).getDecimals(), MyTime.DATETIME_MAX_DECIMALS);

        if (arg0FieldType == FieldTypes.MYSQL_TYPE_DATETIME ||
                arg0FieldType == FieldTypes.MYSQL_TYPE_TIMESTAMP) {
            int dec = Math.max(args.get(0).datetimePrecision(), intervalDec);
            fixLengthAndDecAndCharsetDatetime(MyTime.MAX_DATETIME_WIDTH, dec);
            cachedFieldType = FieldTypes.MYSQL_TYPE_DATETIME;
        } else if (arg0FieldType == FieldTypes.MYSQL_TYPE_DATE) {
            if (intType == SQLIntervalUnit.YEAR || intType == SQLIntervalUnit.QUARTER ||
                    intType == SQLIntervalUnit.MONTH || intType == SQLIntervalUnit.WEEK ||
                    intType == SQLIntervalUnit.DAY || intType == SQLIntervalUnit.YEAR_MONTH) {
                cachedFieldType = FieldTypes.MYSQL_TYPE_DATE;
                fixLengthAndDecAndCharsetDatetime(MyTime.MAX_DATE_WIDTH, 0);
            } else {
                cachedFieldType = FieldTypes.MYSQL_TYPE_DATETIME;
                fixLengthAndDecAndCharsetDatetime(MyTime.MAX_DATE_WIDTH, intervalDec);
            }
        } else if (arg0FieldType == FieldTypes.MYSQL_TYPE_TIME) {
            int dec = Math.max(args.get(0).timePrecision(), intervalDec);
            cachedFieldType = FieldTypes.MYSQL_TYPE_TIME;
            fixLengthAndDecAndCharsetDatetime(MyTime.MAX_TIME_WIDTH, dec);
        } else {
            cachedFieldType = FieldTypes.MYSQL_TYPE_STRING;
            /* Behave as a usual string function when return type is VARCHAR. */
            // fix_length_and_charset(MyTime.MAX_DATETIME_FULL_WIDTH);
        }
    }

    /* Here arg[1] is a Item_interval object */
    private boolean getDateInternal(MySQLTime ltime, long fuzzyDate) {
        Interval interval = new Interval();

        if (args.get(0).getDate(ltime, MyTime.TIME_NO_ZERO_DATE) ||
                MyTime.getIntervalValue(args.get(1), intType, strValue, interval))
            return (nullValue = true);

        if (dateSubInterval)
            interval.setNeg(!interval.isNeg());

        /*
         * Make sure we return proper time_type. It's important for val_str().
         */
        if (cachedFieldType == FieldTypes.MYSQL_TYPE_DATE &&
                ltime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME)
            MyTime.datetimeToDate(ltime);
        else if (cachedFieldType == FieldTypes.MYSQL_TYPE_DATETIME &&
                ltime.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_DATE)
            MyTime.dateToDatetime(ltime);

        return (nullValue = MyTime.dateAddInterval(ltime, intType, interval));
    }

    private boolean getTimeInternal(MySQLTime ltime) {
        Interval interval = new Interval();
        if ((nullValue = args.get(0).getTime(ltime) ||
                MyTime.getIntervalValue(args.get(1), intType, strValue, interval)))
            return true;

        if (dateSubInterval)
            interval.setNeg(!interval.isNeg());

        long usec1 = ((((ltime.getDay() * 24 + ltime.getHour()) * 60 + ltime.getMinute()) * 60 + ltime.getSecond()) * 1000000L +
                ltime.getSecondPart()) * (ltime.isNeg() ? -1 : 1);
        long usec2 = ((((interval.getDay() * 24 + interval.getHour()) * 60 + interval.getMinute()) * 60 + interval.getSecond()) * 1000000L +
                interval.getSecondPart()) * (interval.isNeg() ? -1 : 1);
        long diff = usec1 + usec2;
        LLDivT seconds = new LLDivT();
        seconds.setQuot(diff / 1000000);
        seconds.setRem(diff % 1000000 * 1000); /* time.second_part= lldiv.rem / 1000 */
        if ((nullValue = (interval.getYear() != 0 || interval.getMonth() != 0 || MyTime.secToTime(seconds, ltime)))) {
            LOGGER.info("datetime function overflow!");
            return true;
        }
        return false;
    }

    @Override
    protected boolean valDatetime(MySQLTime ltime, long fuzzyDate) {
        if (cachedFieldType != FieldTypes.MYSQL_TYPE_TIME)
            return getDateInternal(ltime, fuzzyDate | MyTime.TIME_NO_ZERO_DATE);
        return getTimeInternal(ltime);
    }

    @Override
    public SQLExpr toExpression() {
        String funcName = funcName();
        if (dateSubInterval) {
            funcName = "date_sub";
        }
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr(funcName);
        method.addParameter(args.get(0).toExpression());
        SQLIntervalExpr intervalExpr = new SQLIntervalExpr();
        intervalExpr.setValue(args.get(1).toExpression());
        intervalExpr.setUnit(intType);
        method.addParameter(intervalExpr);
        return method;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemDateAddInterval(newArgs.get(0), newArgs.get(1), intType, this.dateSubInterval, charsetIndex);
    }

}
