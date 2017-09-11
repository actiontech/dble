/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.field.temporal;

public abstract class FieldTemporalWithDateAndTime extends FieldTemporaWithDate {

    public FieldTemporalWithDateAndTime(String name, String table, int charsetIndex, int fieldLength,
                                        int decimals, long flags) {
        super(name, table, charsetIndex, fieldLength, decimals, flags);
    }

}
