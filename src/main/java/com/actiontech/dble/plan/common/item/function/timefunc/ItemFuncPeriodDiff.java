/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemIntFunc;
import com.actiontech.dble.plan.common.time.MyTime;

import java.math.BigInteger;
import java.util.List;

public class ItemFuncPeriodDiff extends ItemIntFunc {

    public ItemFuncPeriodDiff(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "period_diff";
    }

    @Override
    public BigInteger valInt() {
        long period1 = args.get(0).valInt().longValue();
        long period2 = args.get(1).valInt().longValue();

        if ((nullValue = args.get(0).isNullValue() || args.get(1).isNullValue()))
            return BigInteger.ZERO; /* purecov: inspected */
        return BigInteger.valueOf(MyTime.convertPeriodToMonth(period1) - MyTime.convertPeriodToMonth(period2));
    }

    @Override
    public void fixLengthAndDec() {
        maxLength = 6;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncPeriodDiff(realArgs);
    }

}
