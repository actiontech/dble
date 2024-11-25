/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.operator.cmpfunc.util;

import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.ptr.BoolPtr;

public interface GetValueFunc {
    long get(Item arg, Item warnitem, BoolPtr isNull);
}
