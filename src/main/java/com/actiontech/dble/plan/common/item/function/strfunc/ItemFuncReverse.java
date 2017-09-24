/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.util.List;


public class ItemFuncReverse extends ItemStrFunc {

    public ItemFuncReverse(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "reverse";
    }

    @Override
    public String valStr() {
        String old = args.get(0).valStr();
        if (args.get(0).isNull()) {
            this.nullValue = true;
            return EMPTY;
        }
        StringBuilder sb = new StringBuilder(old);
        return sb.reverse().toString();
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncReverse(realArgs);
    }
}
