package io.mycat.plan.common.item.function.primary;

import java.util.List;

import io.mycat.plan.common.item.Item;


public abstract class ItemDecFunc extends ItemRealFunc {

	public ItemDecFunc(List<Item> args) {
		super(args);
	}

	@Override
	public void fixLengthAndDec() {
		decimals = NOT_FIXED_DEC;
		maxLength = floatLength(decimals);
		maybeNull = true;
	}
}
