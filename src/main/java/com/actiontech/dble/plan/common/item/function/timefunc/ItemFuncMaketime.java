/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MySQLTimestampType;
import com.actiontech.dble.plan.common.time.MyTime;

import java.math.BigDecimal;
import java.util.List;

public class ItemFuncMaketime extends ItemTimeFunc {

    public ItemFuncMaketime(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "maketime";
    }

    @Override
    public void fixLengthAndDec() {
        maybeNull = true;
        fixLengthAndDecAndCharsetDatetime(MyTime.MAX_TIME_WIDTH, args.get(2).getDecimals());
    }

    /**
     * MAKETIME(h,m,s) is a time function that calculates a time value from the
     * total number of hours, minutes, and seconds. Result: Time value
     */
    @Override
    public boolean getTime(MySQLTime ltime) {
        long minute = args.get(1).valInt().longValue();
        BigDecimal sec = args.get(2).valDecimal();
        if ((nullValue = (args.get(0).isNullValue() || args.get(1).isNullValue() || args.get(2).isNullValue() || sec == null ||
                minute < 0 || minute > 59))) {
            return true;
        }
        long scdquot = sec.longValue();
        long scdrem = (long) ((sec.doubleValue() - scdquot) * 1000000);
        if ((nullValue = (scdquot < 0 || scdquot > 59 || scdrem < 0))) {
            return true;
        }

        ltime.setZeroTime(MySQLTimestampType.MYSQL_TIMESTAMP_TIME);

        long hour = args.get(0).valInt().longValue();
        /* Check for integer overflows */
        if (hour < 0) {
            ltime.setNeg(true);
        }
        ltime.setHour(((hour < 0 ? -hour : hour)));
        ltime.setMinute(minute);
        ltime.setSecond(scdquot);
        ltime.setSecondPart(scdrem);
        return false;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncMaketime(realArgs);
    }

}
