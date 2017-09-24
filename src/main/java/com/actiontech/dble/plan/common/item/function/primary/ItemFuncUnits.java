/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.primary;

import com.actiontech.dble.plan.common.item.Item;

import java.math.BigDecimal;
import java.util.List;


public abstract class ItemFuncUnits extends ItemRealFunc {

    BigDecimal mul, add;

    public ItemFuncUnits(List<Item> args, double mulArg, double addArg) {
        super(args);
        mul = new BigDecimal(mulArg);
        add = new BigDecimal(addArg);
    }

    @Override
    public BigDecimal valReal() {
        BigDecimal value = args.get(0).valReal();
        if ((nullValue = args.get(0).isNullValue()))
            return BigDecimal.ZERO;
        return value.multiply(mul).add(add);
    }

    @Override
    public void fixLengthAndDec() {
        decimals = NOT_FIXED_DEC;
        maxLength = floatLength(decimals);
    }

}
