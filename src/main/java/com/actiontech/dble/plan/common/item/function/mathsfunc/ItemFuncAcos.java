/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemDecFunc;

import java.math.BigDecimal;
import java.util.List;


public class ItemFuncAcos extends ItemDecFunc {

    public ItemFuncAcos(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "acos";
    }

    public BigDecimal valReal() {
        double db = args.get(0).valReal().doubleValue();
        if (nullValue = (args.get(0).isNull() || (db < -1.0 || db > 1.0))) {
            return BigDecimal.ZERO;
        } else {
            return new BigDecimal(Math.acos(db));
        }
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncAcos(realArgs);
    }
}
