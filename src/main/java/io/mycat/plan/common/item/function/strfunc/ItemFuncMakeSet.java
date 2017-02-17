package io.mycat.plan.common.item.function.strfunc;

import java.util.List;

import io.mycat.plan.common.item.Item;


public class ItemFuncMakeSet extends ItemStrFunc {

	public ItemFuncMakeSet(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "make_set";
	}

	@Override
	public String valStr() {
		throw new RuntimeException("not supportted yet!");
	}
}
