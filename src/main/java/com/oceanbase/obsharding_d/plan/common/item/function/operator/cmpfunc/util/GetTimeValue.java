/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.operator.cmpfunc.util;

import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.ptr.BoolPtr;

public class GetTimeValue implements GetValueFunc {

    @Override
    public long get(Item item, Item warn, BoolPtr isNull) {
        long value = item.valTimeTemporal();
        isNull.set(item.isNullValue());
        return value;
    }

}
