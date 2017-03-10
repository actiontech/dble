package io.mycat.plan.common.item.function.timefunc;

import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;
import io.mycat.plan.common.time.MySQLTime;

public class ItemFuncMinute extends ItemIntFunc {

	public ItemFuncMinute(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "minute";
	}

	@Override
	public BigInteger valInt() {
		MySQLTime ltime = new MySQLTime();
		return getArg0Time(ltime) ? BigInteger.ZERO : BigInteger.valueOf(ltime.minute);
	}

	@Override
	public void fixLengthAndDec() {
		maxLength = (2); /* 0..59 */
		maybeNull = true;
	}

}
