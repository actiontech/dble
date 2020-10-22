/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemFuncUnits;

import java.util.List;


public class ItemFuncDegree extends ItemFuncUnits {

    public ItemFuncDegree(List<Item> args, int charsetIndex) {
        super(args, 180 / MySQLcom.M_PI, 0.0, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "degrees";
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncDegree(realArgs, charsetIndex);
    }
}
