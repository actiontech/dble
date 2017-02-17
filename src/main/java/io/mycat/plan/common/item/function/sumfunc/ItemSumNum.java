package io.mycat.plan.common.item.function.sumfunc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.time.MySQLTime;


public abstract class ItemSumNum extends ItemSum {

	public ItemSumNum(List<Item> args, boolean isPushDown, List<Field> fields) {
		super(args, isPushDown, fields);
	}

	/* 是否已经被计算过 */
	boolean is_evaluated;

	@Override
	public boolean fixFields() {
		decimals = 0;
		maybeNull = false;
		for (int i = 0; i < getArgCount(); i++) {
			if (!args.get(i).fixed && args.get(i).fixFields())
				return true;
			decimals = Math.max(decimals, args.get(i).decimals);
			maybeNull |= args.get(i).maybeNull;
		}
		maxLength = floatLength(decimals);
		nullValue = true;
		fixLengthAndDec();
		fixed = true;
		return false;
	}

	@Override
	public BigInteger valInt() {
		return valReal().toBigInteger();
	}

	@Override
	public String valStr() {
		return valStringFromReal();
	}

	@Override
	public BigDecimal valDecimal() {
		return valDecimalFromReal();
	}

	@Override
	public boolean getDate(MySQLTime ltime, long fuzzydate) {
		return getDateFromNumeric(ltime, fuzzydate);
	}

	@Override
	public boolean getTime(MySQLTime ltime) {
		return getTimeFromNumeric(ltime);
	}

}
