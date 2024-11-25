/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.strfunc;

import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;

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
        if (this.nullValue = (args.get(0).isNull() || args.get(1).isNull() ||
                (args.size() == 3 && args.get(2).isNull())))
            return EMPTY;
        long start = args.get(1).valInt().longValue();
        start = (start < 0) ? str.length() + start : start - 1;
        if (start < 0 || start >= str.length()) {
            return EMPTY;
        }
        long length = args.size() == 3 ? args.get(2).valInt().longValue() + start : Long.MAX_VALUE;
        length = Math.min(length, str.length());
        if (length <= 0)
            return EMPTY;
        return str.substring((int) start, (int) length);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncSubstr(realArgs, charsetIndex);
    }
}
