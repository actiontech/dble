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


public class ItemFuncBitLength extends ItemIntFunc {

    public ItemFuncBitLength(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "bit_length";
    }

    @Override
    public BigInteger valInt() {
        String res = args.get(0).valStr();
        if (res == null) {
            nullValue = true;
            return null; /* purecov: inspected */
        }
        nullValue = false;
        return BigInteger.valueOf(res.length() * 8);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncBitLength(realArgs);
    }
}
