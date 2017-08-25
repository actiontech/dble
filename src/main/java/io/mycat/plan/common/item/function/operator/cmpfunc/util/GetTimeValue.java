package io.mycat.plan.common.item.function.operator.cmpfunc.util;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.ptr.BoolPtr;

public class GetTimeValue implements GetValueFunc {

    @Override
    public long get(Item item, Item warn, BoolPtr isNull) {
        long value = item.valTimeTemporal();
        isNull.set(item.isNullValue());
        return value;
    }

}
