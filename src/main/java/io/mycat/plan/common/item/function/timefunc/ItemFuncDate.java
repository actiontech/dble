package io.mycat.plan.common.item.function.timefunc;

import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.time.MySQLTime;


public class ItemFuncDate extends ItemDateFunc {

	public ItemFuncDate(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "date";
	}

	@Override
	public boolean getDate(MySQLTime ltime, long fuzzy_date) {
		if (nullValue = args.get(0).nullValue) {
			return true;
		}
		nullValue = getArg0Date(ltime, fuzzy_date);
		return nullValue;
	}
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncDate(realArgs);
	}
}
