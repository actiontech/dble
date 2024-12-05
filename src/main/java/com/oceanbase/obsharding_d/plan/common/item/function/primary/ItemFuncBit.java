/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.primary;

import com.oceanbase.obsharding_d.plan.common.item.Item;

public abstract class ItemFuncBit extends ItemIntFunc {

    public ItemFuncBit(Item a, int charsetIndex) {
        super(a, charsetIndex);
    }

    public ItemFuncBit(Item a, Item b, int charsetIndex) {
        super(a, b, charsetIndex);
    }

}
