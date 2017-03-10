package io.mycat.plan.common.item.function.mathsfunc;

import java.util.List;

import io.mycat.plan.common.item.Item;

public class ItemFuncTruncate extends ItemFuncRoundOrTruncate {

	public ItemFuncTruncate(List<Item> args) {
		super(args, true);
	}

}
