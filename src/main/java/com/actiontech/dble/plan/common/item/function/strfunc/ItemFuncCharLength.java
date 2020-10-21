/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemIntFunc;

import java.math.BigInteger;
import java.util.List;


public class ItemFuncCharLength extends ItemIntFunc {

    public ItemFuncCharLength(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "char_length";
    }

    @Override
    public BigInteger valInt() {
        String s = args.get(0).valStr();
        if (s == null) {
            this.nullValue = true;
            return BigInteger.ZERO;
        } else {
            nullValue = false;
            return BigInteger.valueOf(s.toCharArray().length);
        }
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncCharLength(realArgs, charsetIndex);
    }
}
