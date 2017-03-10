package io.mycat.plan.common.item.function.mathsfunc;

import java.math.BigDecimal;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemDecFunc;


public class ItemFuncCos extends ItemDecFunc {

	public ItemFuncCos(List<Item> args) {
		super(args);
	}
	
	@Override
	public final String funcName(){
		return "cos";
	}

	public BigDecimal valReal() {
		double db = args.get(0).valReal().doubleValue();
		if (args.get(0).isNull()) {
			this.nullValue = true;
			return BigDecimal.ZERO;
		} else {
			return new BigDecimal(Math.cos(db));
		}
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncCos(realArgs);
	}
}
