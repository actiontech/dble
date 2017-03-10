/**
 * 
 */
package io.mycat.plan.common.item.function.operator.cmpfunc;

import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.operator.ItemBoolFunc2;


public class ItemFuncStrcmp extends ItemBoolFunc2 {

	/**
	 * @param name
	 * @param a
	 * @param b
	 */
	public ItemFuncStrcmp(Item a, Item b) {
		super(a, b);
	}

	@Override
	public final String funcName() {
		return "strcmp";
	}

	@Override
	public BigInteger valInt() {
		String a = args.get(0).valStr();
		String b = args.get(1).valStr();
		if (a == null || b == null) {
			nullValue = true;
			return BigInteger.ZERO;
		}
		int value = a.compareTo(b);
		nullValue = false;
		return value == 0 ? BigInteger.ZERO : (value < 0 ? BigInteger.valueOf(-1) : BigInteger.ONE);
	}

	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncStrcmp(realArgs.get(0), realArgs.get(1));
	}
}
