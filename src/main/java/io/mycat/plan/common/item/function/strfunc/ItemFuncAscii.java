package io.mycat.plan.common.item.function.strfunc;

import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;


public class ItemFuncAscii extends ItemIntFunc {

	public ItemFuncAscii(List<Item> args) {
		super(args);
	}
	
	@Override
	public final String funcName() {
		return "ascii";
	}

	@Override
	public BigInteger valInt() {
		String s = args.get(0).valStr();
		if (args.get(0).isNull()) {
			this.nullValue = true;
			return BigInteger.ZERO;
		} else {
			if (s.length() == 0) {
				return BigInteger.ZERO;
			} else {
				return BigInteger.valueOf((int) s.charAt(0));
			}
		}
	}
}
