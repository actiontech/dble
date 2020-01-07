/*
 * Copyright (C) 2016-2020 ActionTech.
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

public class ItemFuncWeekofyear extends ItemIntFunc {

    public ItemFuncWeekofyear(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "weekofyear";
    }

    @Override
    public BigInteger valInt() {
        MySQLTime ltime = new MySQLTime();
        if (nullValue = getArg0Date(ltime, MyTime.TIME_FUZZY_DATE)) {
            return BigInteger.ZERO;
        }
        return BigInteger.valueOf(ltime.getMonth());
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncWeekofyear(realArgs);
    }
}
