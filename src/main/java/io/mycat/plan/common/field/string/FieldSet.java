package io.mycat.plan.common.field.string;

import io.mycat.plan.common.item.FieldTypes;

public class FieldSet extends FieldEnum {

    public FieldSet(String name, String table, int charsetIndex, int field_length, int decimals, long flags) {
        super(name, table, charsetIndex, field_length, decimals, flags);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_SET;
    }

}
