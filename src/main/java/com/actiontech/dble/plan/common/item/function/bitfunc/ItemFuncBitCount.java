/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.bitfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemIntFunc;

import java.math.BigInteger;
import java.util.List;


/**
 * parameter is any type <br>
 * return BIG INT
 *
 * @author Administrator
 */
public class ItemFuncBitCount extends ItemIntFunc {

    public ItemFuncBitCount(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "bit_count";
    }

    @Override
    public BigInteger valInt() {
        BigInteger value = args.get(0).valInt();
        if ((nullValue = args.get(0).isNullValue()))
            return BigInteger.ZERO; /* purecov: inspected */
        return BigInteger.valueOf(value.bitCount());
    }

    @Override
    public void fixLengthAndDec() {
        maxLength = 2;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncBitCount(realArgs, charsetIndex);
    }
}
