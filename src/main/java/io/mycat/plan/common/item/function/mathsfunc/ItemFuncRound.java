package io.mycat.plan.common.item.function.mathsfunc;

import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;


public class ItemFuncRound extends ItemFuncRoundOrTruncate {

	public ItemFuncRound(List<Item> args) {
		super(args, false);
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncRound(realArgs);
	}

}
