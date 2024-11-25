/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.timefunc;

import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.plan.common.time.MySQLTime;

import java.util.List;


public class ItemFuncCurdateUtc extends ItemDateFunc {

    public ItemFuncCurdateUtc(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "utc_date";
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzyDate) {
        java.util.Calendar cal = getUTCTime();
        ltime.setYear(cal.get(java.util.Calendar.YEAR));
        ltime.setMonth(cal.get(java.util.Calendar.MONTH) + 1);
        ltime.setDay(cal.get(java.util.Calendar.DAY_OF_MONTH));
        ltime.setSecondPart(0);
        ltime.setSecond(0);
        ltime.setMinute(0);
        ltime.setHour(0);
        return false;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncCurdateUtc(realArgs, charsetIndex);
    }
}
