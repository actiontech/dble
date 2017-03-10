package io.mycat.plan.common.item.function.strfunc;

import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;


public class ItemFuncOrd extends ItemIntFunc {

	public ItemFuncOrd(List<Item> args) {
		super(args);
	}
	
	@Override
	public final String funcName(){
		return "ord";
	}
	

	@Override
	public BigInteger valInt() {
		String sub = args.get(0).valStr();
		String str = args.get(1).valStr();
		int pos = -1;
		if (args.size() == 3) {
			pos = (int) args.get(2).valInt().intValue();
		}
		if (args.get(0).isNull() || args.get(1).isNull()) {
			this.nullValue = true;
			return BigInteger.ZERO;
		}
		if (pos <= 1) {
			return BigInteger.valueOf(str.indexOf(sub) + 1);
		} else {
			String posStr = str.substring(pos - 1);
			int find = posStr.indexOf(sub);
			if (find < 0)
				return BigInteger.ZERO;
			else
				return BigInteger.valueOf(pos + find);
		}
	}

}
