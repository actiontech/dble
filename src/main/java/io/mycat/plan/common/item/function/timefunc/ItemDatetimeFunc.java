package io.mycat.plan.common.item.function.timefunc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;


public abstract class ItemDatetimeFunc extends ItemTemporalFunc {

	public ItemDatetimeFunc(List<Item> args) {
		super(args);
	}

	@Override
	public FieldTypes fieldType() {
		return FieldTypes.MYSQL_TYPE_DATETIME;
	}

	@Override
	public BigDecimal valReal() {
		return valRealFromDecimal();
	}

	@Override
	public String valStr() {
		return valStringFromDatetime();
	}

	@Override
	public BigInteger valInt() {
		return BigInteger.valueOf(valIntFromDatetime());
	}

	@Override
	public long valDateTemporal() {
		MySQLTime ltime = new MySQLTime();
		return getDate(ltime, MyTime.TIME_FUZZY_DATE) ? 0L : MyTime.TIME_to_longlong_datetime_packed(ltime);
	}

	@Override
	public BigDecimal valDecimal() {
		return valDecimalFromDate();
	}

	@Override
	public boolean getTime(MySQLTime ltime) {
		return getTimeFromDatetime(ltime);
	}

	// All datetime functions must implement get_date()
	// to avoid use of generic Item::get_date()
	// which converts to string and then parses the string as DATETIME.
	public abstract boolean getDate(MySQLTime res, long fuzzy_date);

}
