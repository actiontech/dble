package io.mycat.plan.common.item.function.operator.cmpfunc.util;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.ptr.BoolPtr;

public interface GetValueFunc {
	long get(Item arg, Item warnitem, BoolPtr isNull);
}