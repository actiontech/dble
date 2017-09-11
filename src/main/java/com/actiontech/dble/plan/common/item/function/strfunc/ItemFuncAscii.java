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


public class ItemFuncAscii extends ItemIntFunc {

    public ItemFuncAscii(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "ascii";
    }

    @Override
    public BigInteger valInt() {
        String s = args.get(0).valStr();
        if (args.get(0).isNull()) {
            this.nullValue = true;
            return BigInteger.ZERO;
        } else {
            if (s.length() == 0) {
                return BigInteger.ZERO;
            } else {
                return BigInteger.valueOf((int) s.charAt(0));
            }
        }
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncAscii(realArgs);
    }
}
