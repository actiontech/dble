/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemIntFunc;
import com.actiontech.dble.plan.common.ptr.LongPtr;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MyTime;

import java.math.BigInteger;
import java.util.List;

public class ItemFuncYearweek extends ItemIntFunc {

    public ItemFuncYearweek(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "yearweek";
    }

    @Override
    public BigInteger valInt() {
        LongPtr year = new LongPtr(0);
        long week;
        MySQLTime ltime = new MySQLTime();
        if (getArg0Date(ltime, MyTime.TIME_NO_ZERO_DATE))
            return BigInteger.ZERO;
        week = MyTime.calcWeek(ltime, (MyTime.weekMode(args.size() > 1 ? args.get(1).valInt().intValue() : 0) | MyTime.WEEK_YEAR), year);
        return BigInteger.valueOf(week + year.get() * 100);
    }

    @Override
    public void fixLengthAndDec() {
        fixCharLength(6); /* YYYYWW */
        maybeNull = true;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncYearweek(realArgs, charsetIndex);
    }
}
