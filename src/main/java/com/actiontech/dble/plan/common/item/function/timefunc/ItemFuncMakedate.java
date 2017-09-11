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

public class ItemFuncMakedate extends ItemDateFunc {

    public ItemFuncMakedate(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "makedate";
    }

    @Override
    /**
     * MAKEDATE(a,b) is a date function that creates a date value from a year
     * and day value.
     *
     * NOTES: As arguments are integers, we can't know if the year is a 2 digit
     * or 4 digit year. In this case we treat all years < 100 as 2 digit years.
     * Ie, this is not safe for dates between 0000-01-01 and 0099-12-31
     */
    public boolean getDate(MySQLTime ltime, long fuzzyDate) {
        long daynr = args.get(1).valInt().longValue();
        long year = args.get(0).valInt().longValue();
        long days;

        if (args.get(0).isNullValue() || args.get(1).isNullValue() || year < 0 || year > 9999 || daynr <= 0) {
            nullValue = true;
            return true;
        }

        if (year < 100)
            year = MyTime.year2000Handling(year);

        days = MyTime.calcDaynr(year, 1, 1) + daynr - 1;
        /* Day number from year 0 to 9999-12-31 */
        if (days >= 0 && days <= MyTime.MAX_DAY_NUMBER) {
            nullValue = false;
            LongPtr lpyear = new LongPtr(0);
            LongPtr lpmonth = new LongPtr(0);
            LongPtr lpday = new LongPtr(0);
            MyTime.getDateFromDaynr(days, lpyear, lpmonth, lpday);
            ltime.setYear(lpyear.get());
            ltime.setMonth(lpmonth.get());
            ltime.setDay(lpday.get());
            ltime.setNeg(false);
            ltime.setSecondPart(0);
            ltime.setSecond(0);
            ltime.setMinute(0);
            ltime.setHour(0);
            ltime.setTimeType(MySQLTimestampType.MYSQL_TIMESTAMP_DATE);
            return false;
        }
        nullValue = true;
        return true;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncMakedate(realArgs);
    }

}
