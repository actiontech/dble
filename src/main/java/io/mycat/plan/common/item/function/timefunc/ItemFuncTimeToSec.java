package io.mycat.plan.common.item.function.timefunc;

import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;
import io.mycat.plan.common.time.MySQLTime;

public class ItemFuncTimeToSec extends ItemIntFunc {

	public ItemFuncTimeToSec(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "time_to_sec";
	}

	@Override
	public BigInteger valInt() {
		MySQLTime ltime = new MySQLTime();
		if (getArg0Time(ltime))
			return BigInteger.ZERO;
		long seconds = ltime.hour * 3600L + ltime.minute * 60 + ltime.second;
		return BigInteger.valueOf(ltime.neg ? -seconds : seconds);
	}

	@Override
	public void fixLengthAndDec() {
		maybeNull = true;
		fixCharLength(10);
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncTimeToSec(realArgs);
	}

}
