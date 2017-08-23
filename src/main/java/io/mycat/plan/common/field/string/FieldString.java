package io.mycat.plan.common.field.string;

import io.mycat.plan.common.item.FieldTypes;

/**
 * char\binary
 *
 * @author ActionTech
 */
public class FieldString extends FieldLongstr {
    public FieldString(String name, String table, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, table, charsetIndex, fieldLength, decimals, flags);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_STRING;
    }

}
