package io.mycat.plan.common.item;

import io.mycat.plan.common.field.Field;

public abstract class ItemResultField extends Item {

	protected ItemResultField() {
		this.withUnValAble = true;
	}


	public void cleanup() {

	}

	public abstract void fixLengthAndDec();

	public abstract String funcName();
}
