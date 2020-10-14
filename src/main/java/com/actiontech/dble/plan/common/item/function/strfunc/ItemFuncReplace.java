/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.util.List;


public class ItemFuncReplace extends ItemStrFunc {

    public ItemFuncReplace(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "replace";
    }

    @Override
    public String valStr() {
        String old = args.get(0).valStr();
        String from = args.get(1).valStr();
        String to = args.get(2).valStr();
        if (args.get(0).isNull() || args.get(1).isNull() || args.get(2).isNull()) {
            this.nullValue = true;
            return EMPTY;
        }
        return old.replace(from, to);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncReplace(realArgs, charsetIndex);
    }
}
