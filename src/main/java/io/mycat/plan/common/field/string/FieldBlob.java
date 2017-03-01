package io.mycat.plan.common.field.string;

import io.mycat.plan.common.item.FieldTypes;

/**
 * 目前blob，enum等值只支持传递，不支持计算
 * 
 * @author ActionTech
 *
 */
public class FieldBlob extends FieldLongstr {

	public FieldBlob(String name, String table, int charsetIndex, int field_length, int decimals, long flags) {
		super(name, table, charsetIndex, field_length, decimals, flags);
	}

	@Override
	public FieldTypes fieldType() {
		return FieldTypes.MYSQL_TYPE_BLOB;
	}

}
