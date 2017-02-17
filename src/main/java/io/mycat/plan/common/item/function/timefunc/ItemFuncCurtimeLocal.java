package io.mycat.plan.common.item.function.timefunc;

import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.time.MySQLTime;


public class ItemFuncCurtimeLocal extends ItemTimeFunc {

	public ItemFuncCurtimeLocal(List<Item> args) {
		super(args);
	}
	
	@Override
	public final String funcName() {
		return "curtime";
	}

	@Override
	public void fixLengthAndDec() {
		/*
		 * We use 8 instead of MAX_TIME_WIDTH (which is 10) because: - there is
		 * no sign - hour is in the 2-digit range
		 */
		fixLengthAndDecAndCharsetDatetime(8, decimals);
	}

	@Override
	public boolean getTime(MySQLTime ltime) {
		java.util.Calendar cal = java.util.Calendar.getInstance();
		ltime.year = ltime.month = ltime.day = 0;
		ltime.hour = cal.get(java.util.Calendar.HOUR_OF_DAY);
		ltime.minute = cal.get(java.util.Calendar.MINUTE);
		ltime.second = cal.get(java.util.Calendar.SECOND);
		ltime.second_part = cal.get(java.util.Calendar.MILLISECOND) * 1000;
		return false;
	}

}
