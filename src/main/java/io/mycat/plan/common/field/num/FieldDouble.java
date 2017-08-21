package io.mycat.plan.common.field.num;

import io.mycat.plan.common.item.FieldTypes;

/**
 * bigint(%d) |unsigned |zerofilled
 *
 * @author ActionTech
 */
public class FieldDouble extends FieldReal {

    public FieldDouble(String name, String table, int charsetIndex, int field_length, int decimals, long flags) {
        super(name, table, charsetIndex, field_length, decimals, flags);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_DOUBLE;
    }

}
