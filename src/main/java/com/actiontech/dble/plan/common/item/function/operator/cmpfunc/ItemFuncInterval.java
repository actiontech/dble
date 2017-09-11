/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.cmpfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemIntFunc;

import java.math.BigInteger;
import java.util.List;


/*
 * INTERVAL(N,N1,N2,N3,...).
 */

public class ItemFuncInterval extends ItemIntFunc {

    public ItemFuncInterval(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "interval";
    }

    @Override
    public void fixLengthAndDec() {
        maybeNull = false;
        maxLength = 2;
    }

    @Override
    public BigInteger valInt() {
        BigInteger arg0 = args.get(0).valInt();
        if (args.get(0).isNullValue())
            return BigInteger.ONE.negate();
        int i = 0;
        for (i = 1; i < args.size(); i++) {
            BigInteger tmp = args.get(i).valInt();
            if (arg0.compareTo(tmp) < 0)
                break;
        }
        return BigInteger.valueOf(i - 1);
    }

    @Override
    public int decimalPrecision() {
        return 2;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncInterval(realArgs);
    }
}
