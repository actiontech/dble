package io.mycat.plan.common.item.function.timefunc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.Item.ItemResult;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.Timeval;

/*
 Abstract class for functions returning "struct timeval".
 */
public abstract class ItemTimevalFunc extends ItemFunc {

	public ItemTimevalFunc(List<Item> args) {
		super(args);
	}

	/**
	 * Return timestamp in "struct timeval" format.
	 * 
	 * @param[out] tm The value is store here.
	 * @retval false On success
	 * @retval true On error
	 */
	public abstract boolean val_timeval(Timeval tm);

	@Override
	public BigDecimal valReal() {
		Timeval tm = new Timeval();
		return val_timeval(tm) ? BigDecimal.ZERO
				: BigDecimal.valueOf((double) tm.tv_sec + (double) tm.tv_usec / (double) 1000000);
	}

	@Override
	public BigInteger valInt() {
		Timeval tm = new Timeval();
		return val_timeval(tm) ? BigInteger.ZERO : BigInteger.valueOf(tm.tv_sec);
	}

	@Override
	public BigDecimal valDecimal() {
		Timeval tm = new Timeval();
		return val_timeval(tm) ? null : tm.timeval2my_decimal();
	}

	@Override
	public String valStr() {
		Timeval tm = new Timeval();
		if (val_timeval(tm))
			return null;
		return tm.my_timeval_to_str();
	}

	@Override
	public boolean getDate(MySQLTime ltime, long fuzzydate) {
		return getDateFromNumeric(ltime, fuzzydate);
	}

	@Override
	public boolean getTime(MySQLTime ltime) {
		return getTimeFromNumeric(ltime);
	}

	public ItemResult resultType() {
		return decimals != 0 ? ItemResult.DECIMAL_RESULT : ItemResult.INT_RESULT;
	}

}
