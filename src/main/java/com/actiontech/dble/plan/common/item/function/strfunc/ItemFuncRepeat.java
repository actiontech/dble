/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.util.List;


public class ItemFuncRepeat extends ItemStrFunc {

    public ItemFuncRepeat(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "repeat";
    }

    @Override
    public String valStr() {
        String old = args.get(0).valStr();
        int count = args.get(1).valInt().intValue();
        if (args.get(0).isNull() || args.get(1).isNull()) {
            this.nullValue = true;
            return EMPTY;
        }
        if (count < 1)
            return EMPTY;
        StringBuilder sb = new StringBuilder();
        for (long l = 0; l < count; l++) {
            sb.append(old);
        }
        return sb.toString();
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncRepeat(realArgs);
    }
}
