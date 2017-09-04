package com.actiontech.dble.plan.common.field.string;

import com.actiontech.dble.plan.common.item.FieldTypes;

public class FieldEnum extends FieldStr {

    public FieldEnum(String name, String table, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, table, charsetIndex, fieldLength, decimals, flags);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_STRING;
    }

}
