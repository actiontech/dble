/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.util.List;

public class ItemFuncSpace extends ItemStrFunc {

    public ItemFuncSpace(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "SPACE";
    }

    @Override
    public String valStr() {
        int count = args.get(0).valInt().intValue();
        if (this.nullValue = args.get(0).isNull())
            return EMPTY;
        if (count <= 0)
            return EMPTY;
        StringBuilder sb = new StringBuilder();
        for (long i = 0; i < count; i++) {
            sb.append(' ');
        }
        return sb.toString();
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncSpace(realArgs);
    }
}
