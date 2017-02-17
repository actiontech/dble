package io.mycat.plan.common.item.function.strfunc;

import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;


public class ItemFuncHex extends ItemStrFunc {

	public ItemFuncHex(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "hex";
	}

	@Override
	public String valStr() {
		Long l = args.get(0).valInt().longValue();
		if (args.get(0).isNull()) {
			this.nullValue = true;
			return EMPTY;
		}
		return Long.toBinaryString(l);
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncHex(realArgs);
	}

}
