package io.mycat.plan.common.item.function.mathsfunc;

import java.util.List;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemFuncUnits;


public class ItemFuncDegree extends ItemFuncUnits {

	public ItemFuncDegree(List<Item> args) {
		super(args, 180 / MySQLcom.M_PI, 0.0);
	}

	@Override
	public final String funcName() {
		return "degrees";
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncDegree(realArgs);
	}
}
