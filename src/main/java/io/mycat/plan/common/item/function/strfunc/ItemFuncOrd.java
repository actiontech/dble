package io.mycat.plan.common.item.function.strfunc;

import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;


public class ItemFuncOrd extends ItemIntFunc {

	public ItemFuncOrd(List<Item> args) {
		super(args);
	}
	
	@Override
	public final String funcName(){
		return "ord";
	}
	

	@Override
	public BigInteger valInt() {
		throw new RuntimeException("not supportted yet!");
	}
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncOrd(realArgs);
	}
}
