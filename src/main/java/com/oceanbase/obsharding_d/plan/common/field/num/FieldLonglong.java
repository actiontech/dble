/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.field.num;

import com.oceanbase.obsharding_d.plan.common.item.FieldTypes;
import com.oceanbase.obsharding_d.plan.common.item.Item;

/**
 * bigint(%d) |unsigned |zerofilled
 *
 * @author oceanbase
 */
public class FieldLonglong extends FieldNum {

    public FieldLonglong(String name, String dbName, String table, String orgTable, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
    }

    @Override
    public Item.ItemResult resultType() {
        return Item.ItemResult.INT_RESULT;
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_LONGLONG;
    }

}
