/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.mathsfunc;

import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.plan.common.item.function.primary.ItemDecFunc;

import java.math.BigDecimal;
import java.util.List;


public class ItemFuncLog10 extends ItemDecFunc {

    public ItemFuncLog10(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "log10";
    }

    public BigDecimal valReal() {
        double value = args.get(0).valReal().doubleValue();

        if ((nullValue = args.get(0).isNullValue()))
            return BigDecimal.ZERO;
        if (value <= 0.0) {
            signalDivideByNull();
            return BigDecimal.ZERO;
        }
        return new BigDecimal(Math.log10(value));
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncLog10(realArgs, charsetIndex);
    }
}
