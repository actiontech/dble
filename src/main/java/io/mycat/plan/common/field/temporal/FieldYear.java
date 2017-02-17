package io.mycat.plan.common.field.temporal;

import io.mycat.plan.common.field.num.FieldTiny;
import io.mycat.plan.common.item.FieldTypes;

/**
 * 
 * @author chenzifei
 *
 */
public class FieldYear extends FieldTiny {

	public FieldYear(String name, String table, int charsetIndex, int field_length, int decimals, long flags) {
		super(name, table, charsetIndex, field_length, decimals, flags);
	}

	@Override
	public FieldTypes fieldType() {
		return FieldTypes.MYSQL_TYPE_YEAR;
	}

}
