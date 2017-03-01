package io.mycat.plan.common.field.num;

import io.mycat.plan.common.item.FieldTypes;

/**
 * decimal(%d,%d) |unsigned |zerofilled
 * 
 * @author ActionTech
 * 
 */
public class FieldDecimal extends FieldReal {

	public FieldDecimal(String name, String table, int charsetIndex, int field_length, int decimals, long flags) {
		super(name, table, charsetIndex, field_length, decimals, flags);
	}

	@Override
	public FieldTypes fieldType() {
		return FieldTypes.MYSQL_TYPE_DECIMAL;
	}

}
