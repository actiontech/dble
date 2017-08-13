package io.mycat.plan.common.item.function.strfunc;

import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;


public class ItemFuncQuote extends ItemStrFunc {

	public ItemFuncQuote(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "quote";
	}

	@Override
	public String valStr() {
		String old = args.get(0).valStr();
		if (args.get(0).isNull()) {
			this.nullValue = true;
			return EMPTY;
		}
		StringBuilder newSb = new StringBuilder();
		for (char c : old.toCharArray()) {
			switch (c) {
			case 0:
			case '\032':
			case '\'':
			case '\\':
				newSb.append('\\').append(c);
				break;
			default:
				newSb.append(c);
				break;
			}
		}
		return newSb.toString();
	}

	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncQuote(realArgs);
	}
}
