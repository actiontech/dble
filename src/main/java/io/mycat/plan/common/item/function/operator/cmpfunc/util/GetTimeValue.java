package io.mycat.plan.common.item.function.operator.cmpfunc.util;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.ptr.BoolPtr;

public class GetTimeValue implements GetValueFunc {

	@Override
	public long get(Item item, Item warn, BoolPtr is_null) {
		long value = item.valTimeTemporal();
		is_null.set(item.nullValue);
		return value;
	}

}
