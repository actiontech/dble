/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.primary;

import com.actiontech.dble.plan.common.item.Item;

public abstract class ItemFuncAdditiveOp extends ItemNumOp {

    public ItemFuncAdditiveOp(Item a, Item b, int charsetIndex) {
        super(a, b, charsetIndex);
    }

    @Override
    public void resultPrecision() {
        decimals = Math.max(args.get(0).getDecimals(), args.get(1).getDecimals());
    }

}
