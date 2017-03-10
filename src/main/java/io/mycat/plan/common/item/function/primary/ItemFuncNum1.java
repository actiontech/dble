package io.mycat.plan.common.item.function.primary;

import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.time.MySQLTime;


/**
 * function where type of result detected by first argument
 * 
 * 
 */
public abstract class ItemFuncNum1 extends ItemFuncNumhybrid {

	public ItemFuncNum1(List<Item> args) {
		super(args);
	}

	@Override
	public void fixNumLengthAndDec() {
		decimals = args.get(0).decimals;
		this.maxLength = args.get(0).maxLength;
	}

	@Override
	public void findNumType() {
		switch (hybrid_type = args.get(0).resultType()) {
		case INT_RESULT:
			break;
		case STRING_RESULT:
		case REAL_RESULT:
			hybrid_type = ItemResult.REAL_RESULT;
			maxLength = floatLength(decimals);
			break;
		case DECIMAL_RESULT:
			break;
		default:
			assert (false);
		}
	}

	@Override
	public String strOp() {
		return null;
	}

	@Override
	public boolean dateOp(MySQLTime ltime, long flags) {
		return false;
	}

	@Override
	public boolean timeOp(MySQLTime ltime) {
		return false;
	}
}
