package io.mycat.plan.common.item.function.mathsfunc;

import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.primary.ItemFuncNum1;


public abstract class ItemFuncIntVal extends ItemFuncNum1 {

	public ItemFuncIntVal(List<Item> args) {
		super(args);
	}

	@Override
	public void fixNumLengthAndDec() {
		decimals = 0;
	}

	@Override
	public void findNumType() {
		switch (hybrid_type = args.get(0).resultType()) {
		case STRING_RESULT:
		case REAL_RESULT:
			hybrid_type = ItemResult.REAL_RESULT;
			maxLength = floatLength(decimals);
			break;
		case INT_RESULT:
			hybrid_type = ItemResult.INT_RESULT;
			break;
		case DECIMAL_RESULT:
			hybrid_type = ItemResult.DECIMAL_RESULT;
			break;
		default:
			assert (false);
		}
	}

}
