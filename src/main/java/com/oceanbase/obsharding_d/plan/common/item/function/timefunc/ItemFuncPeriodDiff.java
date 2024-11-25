/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.timefunc;

import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.plan.common.item.function.primary.ItemIntFunc;
import com.oceanbase.obsharding_d.plan.common.time.MyTime;

import java.math.BigInteger;
import java.util.List;

public class ItemFuncPeriodDiff extends ItemIntFunc {

    public ItemFuncPeriodDiff(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "period_diff";
    }

    @Override
    public BigInteger valInt() {
        long period1 = args.get(0).valInt().longValue();
        long period2 = args.get(1).valInt().longValue();

        if (nullValue = (args.get(0).isNullValue() || args.get(1).isNullValue()))
            return BigInteger.ZERO; /* purecov: inspected */
        return BigInteger.valueOf(MyTime.convertPeriodToMonth(period1) - MyTime.convertPeriodToMonth(period2));
    }

    @Override
    public void fixLengthAndDec() {
        maxLength = 6;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncPeriodDiff(realArgs, charsetIndex);
    }

}
