package io.mycat.plan.common.item.function.operator.cmpfunc;

import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;


/*
 * 至少两个参数,  INTERVAL(N,N1,N2,N3,...).
 * 假如N < N1,则返回值为0;假如N < N2 等等,则返回值为1;假如N 为NULL,则返回值为 -1.
 */

public class ItemFuncInterval extends ItemIntFunc {

	public ItemFuncInterval(List<Item> args) {
		super(args);
	}
	@Override
	public final String funcName(){
		return "interval";
	}

	@Override
	public void fixLengthAndDec() {
		maybeNull = false;
		maxLength = 2;
	}

	@Override
	public BigInteger valInt() {
		BigInteger arg0 = args.get(0).valInt();
		if (args.get(0).nullValue)
			return BigInteger.ONE.negate();
		int i = 0;
		for (i = 1; i < args.size(); i++) {
			BigInteger tmp = args.get(i).valInt();
			if (arg0.compareTo(tmp) < 0)
				break;
		}
		return BigInteger.valueOf(i - 1);
	}

	@Override
	public int decimalPrecision() {
		return 2;
	}
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncInterval(realArgs);
	}
}
