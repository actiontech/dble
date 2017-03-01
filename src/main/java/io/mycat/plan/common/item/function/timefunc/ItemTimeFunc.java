package io.mycat.plan.common.item.function.timefunc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;


/**
 * Abstract class for functions returning TIME values.
 * 
 * @author ActionTech
 * 
 */
public abstract class ItemTimeFunc extends ItemTemporalFunc {

	public ItemTimeFunc(List<Item> args) {
		super(args);
	}

	@Override
	public FieldTypes fieldType() {
		return FieldTypes.MYSQL_TYPE_TIME;
	}

	@Override
	public String valStr() {
		return valStringFromTime();
	}

	@Override
	public BigInteger valInt() {
		return BigInteger.valueOf(valIntFromTime());
	}

	@Override
	public long valDateTemporal() {
		MySQLTime ltime = new MySQLTime();
		return getDate(ltime, MyTime.TIME_FUZZY_DATE) ? 0 : (MyTime.TIME_to_longlong_time_packed(ltime));
	}

	@Override
	public BigDecimal valReal() {
		return valRealFromDecimal();
	}

	@Override
	public void fixLengthAndDec() {
		fixLengthAndDecAndCharsetDatetime(MyTime.MAX_DATE_WIDTH, 0);
	}

	@Override
	public BigDecimal valDecimal() {
		return valDecimalFromTime();
	}

	@Override
	public boolean getDate(MySQLTime res, long fuzzy_date) {
		return getDateFromTime(res);
	}

	// All time functions must implement get_time()
	// to avoid use of generic Item::get_time()
	// which converts to string and then parses the string as TIME.
	public abstract boolean getTime(MySQLTime ltime);
}
