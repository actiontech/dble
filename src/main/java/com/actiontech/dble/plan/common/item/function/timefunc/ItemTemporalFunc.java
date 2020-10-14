/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.util.List;


/**
 * Abstract class for functions returning TIME, DATE, DATETIME types whose data
 * type is known at constructor time.
 */
public abstract class ItemTemporalFunc extends ItemFunc {

    public ItemTemporalFunc(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public ItemResult resultType() {
        return ItemResult.STRING_RESULT;
    }

    public java.util.Calendar getUTCTime() {
        // 1 get local time

        java.util.Calendar cal = java.util.Calendar.getInstance();

        // 2 get ZONE_OFFSET

        int zoneOffset = cal.get(java.util.Calendar.ZONE_OFFSET);

        // 3 get DST_OFFSET

        int dstOffset = cal.get(java.util.Calendar.DST_OFFSET);

        // 4 calu them

        cal.add(java.util.Calendar.MILLISECOND, -(zoneOffset + dstOffset));
        return cal;
    }

}
