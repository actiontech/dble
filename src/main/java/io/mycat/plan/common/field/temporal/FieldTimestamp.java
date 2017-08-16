package io.mycat.plan.common.field.temporal;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.time.MyTime;

public class FieldTimestamp extends FieldTemporalWithDateAndTime {

	public FieldTimestamp(String name, String table, int charsetIndex, int field_length, int decimals, long flags) {
		super(name, table, charsetIndex, field_length, decimals, flags);
	}

	@Override
	public FieldTypes fieldType() {
		return FieldTypes.MYSQL_TYPE_TIMESTAMP;
	}

	@Override
	public BigInteger valInt() {
		internalJob();
		return isNull() ? BigInteger.ZERO : BigInteger.valueOf(MyTime.TIME_to_ulonglong_datetime(ltime));
	}

	@Override
	protected void internalJob() {
		String ptr_str = null;
		try {
			ptr_str = MySQLcom.getFullString(charsetName, ptr);
		} catch (UnsupportedEncodingException ue) {
			logger.warn("parse string exception!", ue);
		}
		if (ptr_str != null)
			MyTime.str_to_datetime_with_warn(ptr_str, ltime, MyTime.TIME_FUZZY_DATE);
	}


}
