package io.mycat.plan.common.item.function.mathsfunc;

import java.math.BigDecimal;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemDecFunc;


public class ItemFuncLn extends ItemDecFunc {

	public ItemFuncLn(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "ln";
	}

	public BigDecimal valReal() {
		double db = args.get(0).valReal().doubleValue();
		if (nullValue = args.get(0).isNull()) {
			return BigDecimal.ZERO;
		}
		if (db <= 0.0) {
			signalDivideByNull();
			return BigDecimal.ZERO;
		} else {
			return new BigDecimal(Math.log(db));
		}
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncLn(realArgs);
	}
}
