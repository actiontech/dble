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


public class ItemFuncPow extends ItemDecFunc {

    public ItemFuncPow(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "pow";
    }

    public BigDecimal valReal() {
        double value = args.get(0).valReal().doubleValue();
        double val2 = args.get(1).valReal().doubleValue();
        if ((nullValue = args.get(0).isNullValue() || args.get(1).isNullValue()))
            return BigDecimal.ZERO;
        return new BigDecimal(Math.pow(value, val2));
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncPow(realArgs);
    }
}
