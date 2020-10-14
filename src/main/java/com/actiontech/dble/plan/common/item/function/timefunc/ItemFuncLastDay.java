/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MySQLTimestampType;
import com.actiontech.dble.plan.common.time.MyTime;

import java.util.List;

public class ItemFuncLastDay extends ItemDateFunc {

    public ItemFuncLastDay(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "last_day";
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzyDate) {
        if ((nullValue = getArg0Date(ltime, fuzzyDate)))
            return true;

        if (ltime.getMonth() == 0) {
            /*
             * Cannot calculate last day for zero month. Let's print a warning
             * and return NULL.
             */
            ltime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_DATE);
            return (nullValue = true);
        }

        int monthIdx = (int) ltime.getMonth() - 1;
        ltime.setDay(MyTime.DAYS_IN_MONTH[monthIdx]);
        if (monthIdx == 1 && MyTime.calcDaysInYear(ltime.getYear()) == 366)
            ltime.setDay(29);
        MyTime.datetimeToDate(ltime);
        return false;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncLastDay(realArgs, charsetIndex);
    }

}
