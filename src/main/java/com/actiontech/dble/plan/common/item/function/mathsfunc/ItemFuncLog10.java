/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemDecFunc;

import java.math.BigDecimal;
import java.util.List;


public class ItemFuncLog10 extends ItemDecFunc {

    public ItemFuncLog10(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "log10";
    }

    public BigDecimal valReal() {
        double value = args.get(0).valReal().doubleValue();

        if ((nullValue = args.get(0).isNullValue()))
            return BigDecimal.ZERO;
        if (value <= 0.0) {
            signalDivideByNull();
            return BigDecimal.ZERO;
        }
        return new BigDecimal(Math.log10(value));
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncLog10(realArgs);
    }
}
