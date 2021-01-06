/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.field.string;

/**
 * base class for Field_string, Field_varstring and Field_blob
 *
 * @author ActionTech
 */
public abstract class FieldLongstr extends FieldStr {

    public FieldLongstr(String name, String dbName, String table, String orgTable, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, dbName, table, orgTable, charsetIndex, fieldLength, decimals, flags);
    }

}
