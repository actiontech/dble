/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemIntFunc;
import com.actiontech.dble.plan.common.time.MyTime;

import java.math.BigInteger;
import java.util.List;

public class ItemFuncPeriodAdd extends ItemIntFunc {

    public ItemFuncPeriodAdd(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "period_add";
    }

    @Override
    public BigInteger valInt() {
        long period = args.get(0).valInt().longValue();
        long months = args.get(1).valInt().longValue();

        if ((nullValue = (args.get(0).isNullValue() || args.get(1).isNullValue())) || period == 0L)
            return BigInteger.ZERO; /* purecov: inspected */
        return BigInteger.valueOf(MyTime.convertMonthToPeriod(MyTime.convertPeriodToMonth(period) + months));
    }

    @Override
    public void fixLengthAndDec() {
        maxLength = 6;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncPeriodAdd(realArgs, charsetIndex);
    }
}
