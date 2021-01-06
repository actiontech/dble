/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.field.string;

import com.actiontech.dble.plan.common.item.FieldTypes;

/**
 * blob,enum is not support calc now
 *
 * @author ActionTech
 */
public class FieldBlob extends FieldLongstr {

    public FieldBlob(String name, String dbName, String table, String orgTable, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_BLOB;
    }

}
