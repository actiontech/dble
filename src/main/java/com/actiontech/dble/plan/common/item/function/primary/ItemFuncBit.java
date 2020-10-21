/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.primary;

import com.actiontech.dble.plan.common.item.Item;

public abstract class ItemFuncBit extends ItemIntFunc {

    public ItemFuncBit(Item a, int charsetIndex) {
        super(a, charsetIndex);
    }

    public ItemFuncBit(Item a, Item b, int charsetIndex) {
        super(a, b, charsetIndex);
    }

}
