/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemRealFunc;

import java.math.BigDecimal;
import java.util.List;


public class ItemFuncRand extends ItemRealFunc {
    //every conn should have a seed,we just a global seed
    // boolean first_eval; // TRUE if val_real() is called 1st time

    public ItemFuncRand(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "rand";
    }

    public BigDecimal valReal() {
        return new BigDecimal(Math.random());
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncRand(realArgs, charsetIndex);
    }

}
