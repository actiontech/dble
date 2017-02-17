package io.mycat.plan.common.item.function.strfunc;

import java.util.List;

import io.mycat.plan.common.item.Item;


public class ItemfuncLoadFile extends ItemStrFunc {

	public ItemfuncLoadFile(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "load_file";
	}

	@Override
	public String valStr() {
		throw new RuntimeException("LOAD_FILE function is not realized");
	}
}
