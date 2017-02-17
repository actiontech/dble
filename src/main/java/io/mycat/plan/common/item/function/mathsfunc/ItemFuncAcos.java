package io.mycat.plan.common.item.function.mathsfunc;

import java.math.BigDecimal;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemDecFunc;


public class ItemFuncAcos extends ItemDecFunc {

	public ItemFuncAcos(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "acos";
	}

	public BigDecimal valReal() {
		double db = args.get(0).valReal().doubleValue();
		if (nullValue = (args.get(0).isNull() || (db < -1.0 || db > 1.0))) {
			return BigDecimal.ZERO;
		} else {
			return new BigDecimal(Math.acos(db));
		}
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncAcos(realArgs);
	}
}
