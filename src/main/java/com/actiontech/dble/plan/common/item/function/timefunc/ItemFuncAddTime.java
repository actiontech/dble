/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.ptr.LongPtr;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MySQLTimestampType;
import com.actiontech.dble.plan.common.time.MyTime;

import java.util.List;

/*
 *  ADDTIME(expr1,expr2)
 *  ADDTIME() adds expr2 to expr1 and returns the result.
 *  expr1 is a time or datetime expression, and expr2 is a time expression.
 */
public class ItemFuncAddTime extends ItemTemporalHybridFunc {

    boolean isDate = false;
    int sign = 1;

    public ItemFuncAddTime(List<Item> args, boolean typeArg, boolean negArg) {
        super(args);
        this.isDate = typeArg;
        this.sign = negArg ? -1 : 1;
    }

    @Override
    public final String funcName() {
        return this.sign == 1 ? "addtime" : "subtime";
    }

    @Override
    public void fixLengthAndDec() {
        /*
         * The field type for the result of an Item_func_add_time function is
         * defined as follows:
         *
         * - If first arg is a MYSQL_TYPE_DATETIME or MYSQL_TYPE_TIMESTAMP
         * result is MYSQL_TYPE_DATETIME - If first arg is a MYSQL_TYPE_TIME
         * result is MYSQL_TYPE_TIME - Otherwise the result is MYSQL_TYPE_STRING
         *
         * TODO: perhaps it should also return MYSQL_TYPE_DATETIME when the
         * first argument is MYSQL_TYPE_DATE.
         */
        if (args.get(0).fieldType() == FieldTypes.MYSQL_TYPE_TIME && !isDate) {
            cachedFieldType = FieldTypes.MYSQL_TYPE_TIME;
        } else if (args.get(0).isTemporalWithDateAndTime() || isDate) {
            cachedFieldType = FieldTypes.MYSQL_TYPE_DATETIME;
        } else {
            cachedFieldType = FieldTypes.MYSQL_TYPE_STRING;
        }
        maybeNull = true;
    }

    @Override
    protected boolean valDatetime(MySQLTime time, long fuzzyDate) {
        MySQLTime lTime1 = new MySQLTime();
        MySQLTime lTime2 = new MySQLTime();
        boolean isTime = false;
        int lSign = sign;

        nullValue = false;
        if (cachedFieldType == FieldTypes.MYSQL_TYPE_DATETIME) /* TIMESTAMP function */ {
            if (getArg0Date(lTime1, fuzzyDate) || args.get(1).getTime(lTime2) ||
                    lTime1.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_TIME ||
                    lTime2.getTimeType() != MySQLTimestampType.MYSQL_TIMESTAMP_TIME) {
                nullValue = true;
                return true;
            }
        } else /* ADDTIME function */ {
            if (args.get(0).getTime(lTime1) || args.get(1).getTime(lTime2) ||
                    lTime2.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME) {
                nullValue = true;
                return true;
            }
            isTime = (lTime1.getTimeType() == MySQLTimestampType.MYSQL_TIMESTAMP_TIME);
        }
        if (lTime1.isNeg() != lTime2.isNeg())
            lSign = -lSign;

        time.setZeroTime(time.getTimeType());

        LongPtr seconds = new LongPtr(0);
        LongPtr microseconds = new LongPtr(0);
        time.setNeg(MyTime.calcTimeDiff(lTime1, lTime2, -lSign, seconds, microseconds));

        /*
         * If first argument was negative and diff between arguments is non-zero
         * we need to swap sign to get proper result.
         */
        if (lTime1.isNeg() && (seconds.get() != 0 || microseconds.get() != 0))
            time.setNeg(!time.isNeg()); // Swap sign of result

        if (!isTime && time.isNeg()) {
            nullValue = true;
            return true;
        }

        long days = seconds.get() / MyTime.SECONDS_IN_24H;

        MyTime.calcTimeFromSec(time, seconds.get() % MyTime.SECONDS_IN_24H, microseconds.get());

        if (!isTime) {
            LongPtr lpyear = new LongPtr(0);
            LongPtr lpmonth = new LongPtr(0);
            LongPtr lpday = new LongPtr(0);
            MyTime.getDateFromDaynr(days, lpyear, lpmonth, lpday);
            time.setYear(lpyear.get());
            time.setMonth(lpmonth.get());
            time.setDay(lpday.get());
            time.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME);
            if (time.getDay() != 0)
                return false;
            nullValue = true;
            return true;
        }
        time.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_TIME);
        time.setHour(time.getHour() + days * 24);
        return false;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncAddTime(realArgs, isDate, sign == -1);
    }
}
