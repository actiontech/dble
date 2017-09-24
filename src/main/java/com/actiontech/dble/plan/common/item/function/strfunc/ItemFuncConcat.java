/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.util.List;


public class ItemFuncConcat extends ItemStrFunc {

    public ItemFuncConcat(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "concat";
    }

    @Override
    public String valStr() {
        StringBuilder sb = new StringBuilder();
        for (Item arg : args) {
            if (arg.isNull()) {
                this.nullValue = true;
                return EMPTY;
            }
            String s = arg.valStr();
            sb.append(s);
        }
        return sb.toString();
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncConcat(realArgs);
    }
}
