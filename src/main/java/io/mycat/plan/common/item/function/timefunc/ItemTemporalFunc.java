package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;

import java.util.List;


/**
 * Abstract class for functions returning TIME, DATE, DATETIME types whose data
 * type is known at constructor time.
 */
public abstract class ItemTemporalFunc extends ItemFunc {

    public ItemTemporalFunc(List<Item> args) {
        super(args);
    }

    @Override
    public ItemResult resultType() {
        return ItemResult.STRING_RESULT;
    }

    public java.util.Calendar getUTCTime() {
        // 1、取得本地时间：

        java.util.Calendar cal = java.util.Calendar.getInstance();

        // 2、取得时间偏移量：

        int zoneOffset = cal.get(java.util.Calendar.ZONE_OFFSET);

        // 3、取得夏令时差：

        int dstOffset = cal.get(java.util.Calendar.DST_OFFSET);

        // 4、从本地时间里扣除这些差量，即可以取得UTC时间：

        cal.add(java.util.Calendar.MILLISECOND, -(zoneOffset + dstOffset));
        return cal;
    }

}
