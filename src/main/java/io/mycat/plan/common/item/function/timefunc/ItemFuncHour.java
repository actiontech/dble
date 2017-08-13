package io.mycat.plan.common.item.function.timefunc;

import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;
import io.mycat.plan.common.time.MySQLTime;

public class ItemFuncHour extends ItemIntFunc {

	public ItemFuncHour(List<Item> args) {
		super(args);
	}
	
	@Override
	public final String funcName(){
		return "hour";
	}

	@Override
	public BigInteger valInt() {
		MySQLTime ltime = new MySQLTime();
		return getArg0Time(ltime) ? BigInteger.ZERO : BigInteger.valueOf(ltime.hour);
	}

	@Override
	public void fixLengthAndDec() {
		maxLength = (2); /* 0..23 */
		maybeNull = true;
	}
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncHour(realArgs);
	}
}
