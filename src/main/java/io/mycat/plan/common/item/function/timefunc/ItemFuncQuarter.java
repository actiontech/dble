package io.mycat.plan.common.item.function.timefunc;

import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;

public class ItemFuncQuarter extends ItemIntFunc {

	public ItemFuncQuarter(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "quarter";
	}

	@Override
	public BigInteger valInt() {
		MySQLTime ltime = new MySQLTime();
		if (getArg0Date(ltime, MyTime.TIME_FUZZY_DATE))
			return BigInteger.ZERO;
		return BigInteger.valueOf((ltime.month + 2) / 3);
	}

	@Override
	public void fixLengthAndDec() {
		fixCharLength(1); /* 1..4 */
		maybeNull = true;
	}
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncQuarter(realArgs);
	}
}
