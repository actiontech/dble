package io.mycat.plan.common.item.function.primary;

import java.util.ArrayList;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.time.MySQLTime;

/**
 * Base class for operations like '+', '-', '*'
 * 
 * 
 */
public abstract class ItemNumOp extends ItemFuncNumhybrid {

	public ItemNumOp(Item a, Item b) {
		super(new ArrayList<Item>());
		args.add(a);
		args.add(b);
	}

	/**
	 * 计算结果总长度
	 */
	public abstract void result_precision();

	@Override
	public void findNumType() {
		ItemResult r0 = args.get(0).numericContextResultType();
		ItemResult r1 = args.get(1).numericContextResultType();

		assert (r0 != ItemResult.STRING_RESULT && r1 != ItemResult.STRING_RESULT);

		if (r0 == ItemResult.REAL_RESULT || r1 == ItemResult.REAL_RESULT) {
			/*
			 * Since DATE/TIME/DATETIME data types return
			 * INT_RESULT/DECIMAL_RESULT type codes, we should never get to here
			 * when both fields are temporal.
			 */
			assert (!args.get(0).isTemporal() || !args.get(1).isTemporal());
			countRealLength();
			maxLength = floatLength(decimals);
			hybrid_type = ItemResult.REAL_RESULT;
		} else if (r0 == ItemResult.DECIMAL_RESULT || r1 == ItemResult.DECIMAL_RESULT) {
			hybrid_type = ItemResult.DECIMAL_RESULT;
			result_precision();
		} else {
			assert (r0 == ItemResult.INT_RESULT && r1 == ItemResult.INT_RESULT);
			decimals = 0;
			hybrid_type = ItemResult.INT_RESULT;
			result_precision();
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
