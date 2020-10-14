/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.primary;

import com.actiontech.dble.plan.common.item.Item;

import java.util.List;


public abstract class ItemDecFunc extends ItemRealFunc {

    public ItemDecFunc(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public void fixLengthAndDec() {
        decimals = NOT_FIXED_DEC;
        maxLength = floatLength(decimals);
        maybeNull = true;
    }
}
