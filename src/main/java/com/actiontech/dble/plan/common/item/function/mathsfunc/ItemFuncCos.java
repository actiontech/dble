/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemDecFunc;

import java.math.BigDecimal;
import java.util.List;


public class ItemFuncCos extends ItemDecFunc {

    public ItemFuncCos(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "cos";
    }

    public BigDecimal valReal() {
        double db = args.get(0).valReal().doubleValue();
        if (args.get(0).isNull()) {
            this.nullValue = true;
            return BigDecimal.ZERO;
        } else {
            return new BigDecimal(Math.cos(db));
        }
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncCos(realArgs, charsetIndex);
    }
}
