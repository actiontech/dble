/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.util.List;


public class ItemFuncLower extends ItemStrFunc {

    public ItemFuncLower(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "lower";
    }

    @Override
    public String valStr() {
        String orgStr = args.get(0).valStr();
        if (this.nullValue = args.get(0).isNull())
            return EMPTY;
        return orgStr.toLowerCase();
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncLower(realArgs);
    }
}
