package io.mycat.plan.common.field.num;

import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item.ItemResult;

import java.math.BigDecimal;

/**
 * int(%d) |unsigned |zerofilled
 *
 * @author ActionTech
 */
public class FieldLong extends FieldNum {

    public FieldLong(String name, String table, int charsetIndex, int field_length, int decimals, long flags) {
        super(name, table, charsetIndex, field_length, decimals, flags);
    }

    @Override
    public ItemResult resultType() {
        return ItemResult.INT_RESULT;
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_LONG;
    }

    @Override
    public BigDecimal valDecimal() {
        return new BigDecimal(valInt());
    }

}
