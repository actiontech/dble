/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.field.temporal;

import com.oceanbase.obsharding_d.plan.common.field.num.FieldTiny;
import com.oceanbase.obsharding_d.plan.common.item.FieldTypes;

/**
 * @author ActionTech
 */
public class FieldYear extends FieldTiny {

    public FieldYear(String name, String dbName, String table, String orgTable, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_YEAR;
    }

}
