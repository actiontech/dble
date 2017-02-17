package io.mycat.plan.common.item;

import io.mycat.plan.common.field.Field;

public abstract class ItemResultField extends Item {
	public Field resultField;/* Save result here */

	protected ItemResultField() {
		this.withUnValAble = true;
	}

	public void set_result_field(Field field) {
		this.resultField = field;
	}

	public void cleanup() {
		resultField = null;
	}

	public abstract void fixLengthAndDec();

	public abstract String funcName();
}
