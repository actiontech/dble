package io.mycat.plan.common.item.function.strfunc;

import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;


public class ItemFuncReverse extends ItemStrFunc {

	public ItemFuncReverse(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "reverse";
	}

	@Override
	public String valStr() {
		String old = args.get(0).valStr();
		if (args.get(0).isNull()) {
			this.nullValue = true;
			return EMPTY;
		}
		StringBuilder sb = new StringBuilder(old);
		return sb.reverse().toString();
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncReverse(realArgs);
	}
}
