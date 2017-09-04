package com.actiontech.dble.plan.common.item.function.operator.cmpfunc.util;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.ptr.BoolPtr;

public interface GetValueFunc {
    long get(Item arg, Item warnitem, BoolPtr isNull);
}
