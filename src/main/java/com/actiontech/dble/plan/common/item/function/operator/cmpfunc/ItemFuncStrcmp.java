/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

/**
 *
 */
package com.actiontech.dble.plan.common.item.function.operator.cmpfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.operator.ItemBoolFunc2;

import java.math.BigInteger;
import java.util.List;


public class ItemFuncStrcmp extends ItemBoolFunc2 {

    /**
     * @param a
     * @param b
     */
    public ItemFuncStrcmp(Item a, Item b, int charsetIndex) {
        super(a, b, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "strcmp";
    }

    @Override
    public BigInteger valInt() {
        String a = args.get(0).valStr();
        String b = args.get(1).valStr();
        if (a == null || b == null) {
            nullValue = true;
            return BigInteger.ZERO;
        }
        int value = a.compareTo(b);
        nullValue = false;
        return value == 0 ? BigInteger.ZERO : (value < 0 ? BigInteger.valueOf(-1) : BigInteger.ONE);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncStrcmp(realArgs.get(0), realArgs.get(1), this.charsetIndex);
    }
}
