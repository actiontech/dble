package io.mycat.plan.common.field.num;

import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item.ItemResult;

/**
 * tinyint(%d) |unsigned | zerofilled
 * 
 * @author chenzifei
 *
 */
public class FieldTiny extends FieldNum {

	public FieldTiny(String name, String table, int charsetIndex, int field_length, int decimals, long flags) {
		super(name, table, charsetIndex, field_length, decimals, flags);
	}

	@Override
	public ItemResult resultType() {
		return ItemResult.INT_RESULT;
	}

	@Override
	public FieldTypes fieldType() {
		return FieldTypes.MYSQL_TYPE_TINY;
	}
}
