package com.actiontech.dble.plan.common.field.num;

import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item.ItemResult;

/**
 * mediumint(%d) |unsigned |zerofilled
 *
 * @author ActionTech
 */
public class FieldMedium extends FieldNum {

    public FieldMedium(String name, String table, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, table, charsetIndex, fieldLength, decimals, flags);
    }

    @Override
    public ItemResult resultType() {
        return ItemResult.INT_RESULT;
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_INT24;
    }

}
