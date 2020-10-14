/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemDecFunc;

import java.math.BigDecimal;
import java.util.List;


public class ItemFuncLog2 extends ItemDecFunc {
    private static final double M_LN2 = Math.log(2);

    public ItemFuncLog2(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "log2";
    }

    public BigDecimal valReal() {
        double value = args.get(0).valReal().doubleValue();

        if ((nullValue = args.get(0).isNullValue()))
            return BigDecimal.ZERO;
        if (value <= 0.0) {
            signalDivideByNull();
            return BigDecimal.ZERO;
        }
        return new BigDecimal(Math.log(value) / M_LN2);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncLog2(realArgs, charsetIndex);
    }
}
