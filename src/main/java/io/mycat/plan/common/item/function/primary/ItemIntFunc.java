package io.mycat.plan.common.item.function.primary;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.time.MySQLTime;


public abstract class ItemIntFunc extends ItemFunc {

	public ItemIntFunc(Item a) {
		this(new ArrayList<Item>());
		args.add(a);
	}

	public ItemIntFunc(Item a, Item b) {
		this(new ArrayList<Item>());
		args.add(a);
		args.add(b);
	}

	public ItemIntFunc(List<Item> args) {
		super(args);
	}

	@Override
	public BigDecimal valReal() {
		BigInteger nr = valInt();
		return new BigDecimal(nr);
	}

	@Override
	public String valStr() {
		BigInteger val = valInt();
		if (nullValue)
			return null;
		return val.toString();
	}

	@Override
	public boolean getDate(MySQLTime ltime, long flags) {
		return getDateFromInt(ltime, flags);
	}

	@Override
	public boolean getTime(MySQLTime ltime) {
		return getTimeFromInt(ltime);
	}

	@Override
	public ItemResult resultType() {
		return ItemResult.INT_RESULT;
	}

	@Override
	public void fixLengthAndDec() {

	}

}
