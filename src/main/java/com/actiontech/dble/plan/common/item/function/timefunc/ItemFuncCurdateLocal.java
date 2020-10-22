/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;

import java.util.List;


public class ItemFuncCurdateLocal extends ItemDateFunc {

    public ItemFuncCurdateLocal(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "curdate";
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzyDate) {
        java.util.Calendar cal = java.util.Calendar.getInstance();
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
        return new ItemFuncCurdateLocal(realArgs, charsetIndex);
    }
}
