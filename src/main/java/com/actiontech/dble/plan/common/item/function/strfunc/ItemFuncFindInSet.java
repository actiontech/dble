/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemIntFunc;

import java.math.BigInteger;
import java.util.List;


public class ItemFuncFindInSet extends ItemIntFunc {

    public ItemFuncFindInSet(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "find_in_set";
    }

    @Override
    public BigInteger valInt() {
        long index = 0;
        String s = args.get(0).valStr();
        String source = args.get(1).valStr();
        if (args.get(0).isNull() || args.get(1).isNull()) {
            this.nullValue = true;
            return BigInteger.ZERO;
        }
        if (source.isEmpty())
            return BigInteger.ZERO;
        String[] ss = source.split(",");
        for (int i = 0; i < ss.length; ++i) {
            if (ss[i].equals(s)) {
                index = i + 1;
                break;
            }
        }
        return BigInteger.valueOf(index);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncFindInSet(realArgs);
    }
}
