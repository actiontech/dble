/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.util.List;


public class ItemFuncHex extends ItemStrFunc {

    public ItemFuncHex(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "hex";
    }

    @Override
    public String valStr() {
        Long l = args.get(0).valInt().longValue();
        if (args.get(0).isNull()) {
            this.nullValue = true;
            return EMPTY;
        }
        String val = Long.toHexString(l);
        return val.length() % 2 != 0 ? "0" + val : val;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncHex(realArgs);
    }

}
