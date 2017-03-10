package io.mycat.plan.common.item.function.mathsfunc;

import java.math.BigDecimal;
import java.util.List;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemRealFunc;


public class ItemFuncPi extends ItemRealFunc {

	public ItemFuncPi(List<Item> args) {
		super(args);
	}
	
	@Override
	public final String funcName(){
		return "pi";		
	}

	public BigDecimal valReal() {
		return new BigDecimal(MySQLcom.M_PI);
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncPi(realArgs);
	}
}
