package io.mycat.plan.common.item.function.primary;

import java.util.ArrayList;
import java.util.List;

import io.mycat.plan.common.item.Item;


/**
 * 返回结果boolean的function算子的基类
 * 
 * 
 */
public abstract class ItemBoolFunc extends ItemIntFunc {

	public ItemBoolFunc(Item a) {
		this(new ArrayList<Item>());
		args.add(a);
	}

	public ItemBoolFunc(Item a, Item b) {
		this(new ArrayList<Item>());
		args.add(a);
		args.add(b);
	}

	public ItemBoolFunc(List<Item> args) {
		super(args);
	}

	@Override
	public void fixLengthAndDec() {
		decimals = 0;
		maxLength = 1;
	}

	@Override
	public int decimalPrecision() {
		return 1;
	}

}
