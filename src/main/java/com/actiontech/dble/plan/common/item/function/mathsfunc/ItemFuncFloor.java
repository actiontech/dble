/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;


public class ItemFuncFloor extends ItemFuncIntVal {

    public ItemFuncFloor(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "floor";
    }

    @Override
    public BigInteger intOp() {
        BigInteger result;
        ItemResult i = args.get(0).resultType();
        if (i == ItemResult.INT_RESULT) {
            result = args.get(0).valInt();
            nullValue = args.get(0).isNullValue();

        } else if (i == ItemResult.DECIMAL_RESULT) {
            BigDecimal dec = decimalOp();
            if (dec == null)
                result = BigInteger.ZERO;
            else
                result = dec.toBigInteger();
        } else {
            result = realOp().toBigInteger();
        }
        return result;
    }

    @Override
    public BigDecimal realOp() {
        double value = args.get(0).valReal().doubleValue();
        nullValue = args.get(0).isNullValue();
        return new BigDecimal(Math.floor(value));
    }

    @Override
    public BigDecimal decimalOp() {
        BigDecimal bd = args.get(0).valDecimal();
        if (nullValue = args.get(0).isNullValue())
            return BigDecimal.ZERO;
        return bd.setScale(0, RoundingMode.FLOOR);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncFloor(realArgs, charsetIndex);
    }
}
