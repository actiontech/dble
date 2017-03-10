package io.mycat.plan.common.item.function.mathsfunc;

import java.math.BigDecimal;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemDecFunc;


public class ItemFuncTan extends ItemDecFunc {

	public ItemFuncTan(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "tan";
	}

	public BigDecimal valReal() {
		double db0 = args.get(0).valReal().doubleValue();
		if (args.get(0).isNull()) {
			this.nullValue = true;
			return BigDecimal.ZERO;
		}
		return new BigDecimal(Math.tan(db0));
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncTan(realArgs);
	}
}
