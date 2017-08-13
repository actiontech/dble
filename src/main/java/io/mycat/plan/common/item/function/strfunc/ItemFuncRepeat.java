package io.mycat.plan.common.item.function.strfunc;

import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;


public class ItemFuncRepeat extends ItemStrFunc {

	public ItemFuncRepeat(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "repeat";
	}

	@Override
	public String valStr() {
		String old = args.get(0).valStr();
		int count = args.get(1).valInt().intValue();
		if (args.get(0).isNull() || args.get(1).isNull()) {
			this.nullValue = true;
			return EMPTY;
		}
		if (count < 1)
			return EMPTY;
		StringBuilder sb = new StringBuilder();
		for (long l = 0; l < count; l++) {
			sb.append(old);
		}
		return sb.toString();
	}

	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncRepeat(realArgs);
	}
}
