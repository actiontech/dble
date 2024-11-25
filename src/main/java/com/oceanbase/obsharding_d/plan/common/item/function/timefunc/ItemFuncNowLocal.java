/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.timefunc;

import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.plan.common.time.MySQLTime;
import com.oceanbase.obsharding_d.plan.common.time.MyTime;

import java.util.List;

public class ItemFuncNowLocal extends ItemDatetimeFunc {

    public ItemFuncNowLocal(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "now";
    }

    @Override
    public void fixLengthAndDec() {
        fixLengthAndDecAndCharsetDatetime(MyTime.MAX_DATETIME_WIDTH, decimals);
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzyDate) {
        java.util.Calendar cal = java.util.Calendar.getInstance();
        ltime.setYear(cal.get(java.util.Calendar.YEAR));
        ltime.setMonth(cal.get(java.util.Calendar.MONTH) + 1);
        ltime.setDay(cal.get(java.util.Calendar.DAY_OF_MONTH));
        ltime.setHour(cal.get(java.util.Calendar.HOUR_OF_DAY));
        ltime.setMinute(cal.get(java.util.Calendar.MINUTE));
        ltime.setSecond(cal.get(java.util.Calendar.SECOND));
        ltime.setSecondPart(cal.get(java.util.Calendar.MILLISECOND) * 1000L);
        return false;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncNowLocal(realArgs, charsetIndex);
    }
}
