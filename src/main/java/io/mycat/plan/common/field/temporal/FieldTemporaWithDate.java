package io.mycat.plan.common.field.temporal;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;

/**
 * Abstract class for types with date with optional time, with or without
 * fractional part: DATE, DATETIME, DATETIME(N), TIMESTAMP, TIMESTAMP(N).
 * 
 * @author ActionTech
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
	@Override
	public int compare(byte[] v1, byte[] v2) {
		if (v1 == null && v2 == null)
			return 0;
		else if (v1 == null) {
			return -1;
		} else if (v2 == null) {
			return 1;
		} else
			try {
				String sval1 = MySQLcom.getFullString(charsetName, v1);
				String sval2 = MySQLcom.getFullString(charsetName, v2);
				MySQLTime ltime1 = new MySQLTime();
				MySQLTime ltime2 = new MySQLTime();
				MyTime.str_to_datetime_with_warn(sval1, ltime1, MyTime.TIME_FUZZY_DATE);
				MyTime.str_to_datetime_with_warn(sval2, ltime2, MyTime.TIME_FUZZY_DATE);
				return ltime1.getCompareResult(ltime2);
			} catch (Exception e) {
				logger.info("String to biginteger exception!", e);
				return -1;
			}
	}
}
