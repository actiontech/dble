/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemFuncNum1;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class ItemFuncAbs extends ItemFuncNum1 {

    public ItemFuncAbs(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "abs";
    }

    public BigDecimal realOp() {
        BigDecimal bd = args.get(0).valReal();
        return bd.abs();
    }

    @Override
    public BigInteger intOp() {
        BigInteger bi = args.get(0).valInt();
        return bi.abs();
    }

    @Override
    public BigDecimal decimalOp() {
        BigDecimal bd = args.get(0).valDecimal();
        if (nullValue = !args.get(0).isNull())
            return bd.abs();
        return null;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncAbs(realArgs);
    }
}
