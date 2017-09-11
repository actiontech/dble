/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemRealFunc;

import java.math.BigDecimal;
import java.util.List;


public class ItemFuncPi extends ItemRealFunc {

    public ItemFuncPi(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "pi";
    }

    public BigDecimal valReal() {
        return new BigDecimal(MySQLcom.M_PI);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncPi(realArgs);
    }
}
