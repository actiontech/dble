package io.mycat.plan.common.item.function.strfunc;

import java.util.List;

import io.mycat.plan.common.item.Item;


public class ItemFuncRight extends ItemStrFunc {

	public ItemFuncRight(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "right";
	}

	@Override
	public String valStr() {
		String orgStr = args.get(0).valStr();
		long len = args.get(1).valInt().longValue();
		if (args.get(0).isNull() || args.get(1).isNull()) {
			this.nullValue = true;
			return EMPTY;
		}
		int size = orgStr.length();
		if (len >= size)
			return orgStr;
		return orgStr.substring(size - (int) len, size);
	}

}
