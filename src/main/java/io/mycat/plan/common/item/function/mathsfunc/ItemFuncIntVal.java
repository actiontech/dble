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
		ItemResult i = hybrid_type = args.get(0).resultType();
		if (i == ItemResult.STRING_RESULT || i == ItemResult.REAL_RESULT) {
			hybrid_type = ItemResult.REAL_RESULT;
			maxLength = floatLength(decimals);

		} else if (i == ItemResult.INT_RESULT) {
			hybrid_type = ItemResult.INT_RESULT;

		} else if (i == ItemResult.DECIMAL_RESULT) {
			hybrid_type = ItemResult.DECIMAL_RESULT;

		} else {
			assert (false);
		}
	}

}
