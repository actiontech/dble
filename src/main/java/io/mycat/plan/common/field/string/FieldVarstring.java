package io.mycat.plan.common.field.string;

import io.mycat.plan.common.item.FieldTypes;

public class FieldVarstring extends FieldString {
    public FieldVarstring(String name, String table, int charsetIndex, int field_length, int decimals, long flags) {
        super(name, table, charsetIndex, field_length, decimals, flags);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_VAR_STRING;
    }
}
