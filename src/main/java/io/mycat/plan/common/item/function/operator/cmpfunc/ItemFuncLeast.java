package io.mycat.plan.common.item.function.operator.cmpfunc;

import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;


/**
 * mysql> select least('11', '2'), least('11', '2')+0, concat(least(11,2));<br>
 */
public class ItemFuncLeast extends ItemFuncMinMax {

	public ItemFuncLeast(List<Item> args) {
		super(args, 1);
	}

	@Override
	public final String funcName() {
		return "least";
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncLeast(realArgs);
	}

}
