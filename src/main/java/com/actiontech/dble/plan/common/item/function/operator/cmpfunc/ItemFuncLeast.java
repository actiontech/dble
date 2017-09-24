/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.cmpfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.util.List;


/**
 * mysql> select least('11', '2'), least('11', '2')+0, concat(least(11,2));<br>
 */
public class ItemFuncLeast extends ItemFuncMinMax {

    public ItemFuncLeast(List<Item> args) {
        super(args, 1);
    }

    @Override
    public final String funcName() {
        return "least";
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncLeast(realArgs);
    }

}
