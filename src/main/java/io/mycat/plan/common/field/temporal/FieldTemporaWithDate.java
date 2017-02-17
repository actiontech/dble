package io.mycat.plan.common.field.temporal;

import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;

/**
 * Abstract class for types with date with optional time, with or without
 * fractional part: DATE, DATETIME, DATETIME(N), TIMESTAMP, TIMESTAMP(N).
 * 
 * @author chenzifei
 * 
 */
public abstract class FieldTemporaWithDate extends FieldTemporal {

	public FieldTemporaWithDate(String name, String table, int charsetIndex, int field_length, int decimals,
			long flags) {
		super(name, table, charsetIndex, field_length, decimals, flags);
	}

	@Override
	public long valTimeTemporal() {
		internalJob();
		return isNull() ? 0 : MyTime.TIME_to_longlong_time_packed(ltime);
	}

	@Override
	public long valDateTemporal() {
		internalJob();
		return isNull() ? 0 : MyTime.TIME_to_longlong_datetime_packed(ltime);
	}

	@Override
	public boolean getTime(MySQLTime time) {
		internalJob();
		return isNull() ? true : getDate(time, MyTime.TIME_FUZZY_DATE);
	}

}
