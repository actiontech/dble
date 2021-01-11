/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.field.num;

import com.actiontech.dble.plan.common.item.FieldTypes;

/**
 * decimal(%d,%d) |unsigned |zerofilled
 *
 * @author ActionTech
 */
public class FieldDecimal extends FieldReal {

    public FieldDecimal(String name, String dbName, String table, String orgTable, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_DECIMAL;
    }

}
