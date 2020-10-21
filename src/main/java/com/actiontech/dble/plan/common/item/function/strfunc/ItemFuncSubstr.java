/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.util.List;

public class ItemFuncSubstr extends ItemStrFunc {

    public ItemFuncSubstr(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "SUBSTRING";
    }

    @Override
    public String valStr() {
        String str = args.get(0).valStr();
        long start = args.get(1).valInt().longValue();
        long length = args.size() == 3 ? args.get(2).valInt().longValue() : Long.MAX_VALUE;
        if (this.nullValue = (args.get(0).isNull() || args.get(1).isNull() ||
                (args.size() == 3 && args.get(2).isNull())))
            return EMPTY;
        if (args.size() == 3 && length <= 0)
            return EMPTY;
        start = (start < 0) ? str.length() + start : start - 1;
        long tmpLength = str.length() - start;
        length = Math.min(length, tmpLength);
        if (start == 0 && str.length() == length)
            return EMPTY;
        return str.substring((int) start, (int) (start + length));
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncSubstr(realArgs, charsetIndex);
    }
}
