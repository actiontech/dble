package io.mycat.plan.common.item.function.strfunc;

import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;


public class ItemFuncConcat extends ItemStrFunc {

	public ItemFuncConcat(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "concat";
	}

	@Override
	public String valStr() {
		StringBuilder sb = new StringBuilder();
		for (Item arg : args) {
			if (arg.isNull()) {
				this.nullValue = true;
				return EMPTY;
			}
			String s = arg.valStr();
			sb.append(s);
		}
		return sb.toString();
	}

	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncConcat(realArgs);
	}
}
