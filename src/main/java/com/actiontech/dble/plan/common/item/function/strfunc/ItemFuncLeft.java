/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.util.List;


public class ItemFuncLeft extends ItemStrFunc {

    public ItemFuncLeft(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "left";
    }

    @Override
    public String valStr() {
        String orgStr = args.get(0).valStr();
        long len = args.get(1).valInt().longValue();
        if (args.get(0).isNull() || args.get(1).isNull()) {
            this.nullValue = true;
            return EMPTY;
        }
        int size = orgStr.length();
        if (len >= size)
            return orgStr;
        return orgStr.substring(0, (int) len);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncLeft(realArgs, charsetIndex);
    }
}
