/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.strfunc.ItemStrFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MyTime;

import java.util.List;


public class ItemFuncDayname extends ItemStrFunc {

    public ItemFuncDayname(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "dayname";
    }

    @Override
    public String valStr() {
        MySQLTime ltime = new MySQLTime();
        if (getArg0Date(ltime, MyTime.TIME_NO_ZERO_DATE))
            return null;

        long weekday = MyTime.calcWeekday(MyTime.calcDaynr(ltime.getYear(), ltime.getMonth(), ltime.getDay()), false);
        return MyTime.DAY_NAMES[(int) weekday];
    }

    @Override
    public void fixLengthAndDec() {
        maxLength = 9;
        decimals = 0;
        maybeNull = true;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncDayname(realArgs);
    }

}
