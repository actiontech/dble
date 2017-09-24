/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.cmpfunc.util;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.ptr.BoolPtr;

public class GetTimeValue implements GetValueFunc {

    @Override
    public long get(Item item, Item warn, BoolPtr isNull) {
        long value = item.valTimeTemporal();
        isNull.set(item.isNullValue());
        return value;
    }

}
