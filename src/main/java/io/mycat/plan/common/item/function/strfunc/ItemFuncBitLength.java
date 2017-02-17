package io.mycat.plan.common.item.function.strfunc;

import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;


public class ItemFuncBitLength extends ItemIntFunc {

	public ItemFuncBitLength(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "bit_length";
	}

	@Override
	public BigInteger valInt() {
		String res = args.get(0).valStr();
		if (res == null) {
			nullValue = true;
			return null; /* purecov: inspected */
		}
		nullValue = false;
		return BigInteger.valueOf(res.length() * 8);
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncBitLength(realArgs);
	}
}
