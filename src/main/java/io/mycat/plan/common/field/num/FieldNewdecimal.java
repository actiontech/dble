package io.mycat.plan.common.field.num;

import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item.ItemResult;

/**
 * decimal(%d,%d) |unsigned |zerofilled
 *
 * @author ActionTech
 */
public class FieldNewdecimal extends FieldDecimal {

    public FieldNewdecimal(String name, String table, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, table, charsetIndex, fieldLength, decimals, flags);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_NEWDECIMAL;
    }

    @Override
    public ItemResult resultType() {
        return ItemResult.DECIMAL_RESULT;
    }

}
