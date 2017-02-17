package io.mycat.plan.common.item.function.mathsfunc;

import java.math.BigDecimal;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemDecFunc;


public class ItemFuncAtan extends ItemDecFunc {

	public ItemFuncAtan(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return args.size() == 1 ? "atan" : "atan2";
	}

	public BigDecimal valReal() {
		double value = args.get(0).valReal().doubleValue();
		if ((nullValue = args.get(0).nullValue))
			return BigDecimal.ZERO;
		if (args.size() == 2) {
			double val2 = args.get(1).valReal().doubleValue();
			if ((nullValue = args.get(1).nullValue))
				return BigDecimal.ZERO;
			return new BigDecimal(Math.atan2(value, val2));
		}
		return new BigDecimal(Math.atan(value));
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncAtan(realArgs);
	}
}
