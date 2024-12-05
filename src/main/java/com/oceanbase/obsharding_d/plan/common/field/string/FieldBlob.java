/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.field.string;

import com.oceanbase.obsharding_d.plan.common.item.FieldTypes;

/**
 * blob,enum is not support calc now
 *
 * @author oceanbase
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
