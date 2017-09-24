/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.ptr.LongPtr;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MySQLTimestampType;
import com.actiontech.dble.plan.common.time.MyTime;

import java.util.List;


public class ItemFuncFromDays extends ItemDateFunc {

    public ItemFuncFromDays(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "from_days";
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzyDate) {
        long value = args.get(0).valInt().longValue();
        if ((nullValue = args.get(0).isNullValue()))
            return true;
        ltime.setZeroTime(ltime.getTimeType());
        LongPtr lpyear = new LongPtr(0);
        LongPtr lpmonth = new LongPtr(0);
        LongPtr lpday = new LongPtr(0);
        MyTime.getDateFromDaynr(value, lpyear, lpmonth, lpday);
        ltime.setYear(lpyear.get());
        ltime.setMonth(lpmonth.get());
        ltime.setDay(lpday.get());

        if ((nullValue = ((fuzzyDate & MyTime.TIME_NO_ZERO_DATE) != 0) &&
                (ltime.getYear() == 0 || ltime.getMonth() == 0 || ltime.getDay() == 0)))
            return true;

        ltime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_DATE);
        return false;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncFromDays(realArgs);
    }
}
