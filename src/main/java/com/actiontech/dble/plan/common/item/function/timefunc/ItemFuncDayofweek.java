/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemIntFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MyTime;

import java.math.BigInteger;
import java.util.List;


public class ItemFuncDayofweek extends ItemIntFunc {

    public ItemFuncDayofweek(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "dayofweek";
    }

    @Override
    public BigInteger valInt() {
        MySQLTime ltime = new MySQLTime();
        if (getArg0Date(ltime, MyTime.TIME_FUZZY_DATE)) {
            return BigInteger.ZERO;
        } else {
            java.util.Calendar cal = ltime.toCalendar();
            return BigInteger.valueOf(cal.get(java.util.Calendar.DAY_OF_WEEK));
        }
    }

    @Override
    public void fixLengthAndDec() {
        maxLength = (1); /* 1..31 */
        maybeNull = true;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncDayofweek(realArgs);
    }
}
