package io.mycat.plan.common.item.function.timefunc;

import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;

public class ItemFuncNowLocal extends ItemDatetimeFunc {

	public ItemFuncNowLocal(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "now";
	}
	
	@Override
	public void fixLengthAndDec() {
		fixLengthAndDecAndCharsetDatetime(MyTime.MAX_DATETIME_WIDTH, decimals);
	}

	@Override
	public boolean getDate(MySQLTime ltime, long fuzzy_date) {
		java.util.Calendar cal = java.util.Calendar.getInstance();
		ltime.year = cal.get(java.util.Calendar.YEAR);
		ltime.month = cal.get(java.util.Calendar.MONTH) + 1;
		ltime.day = cal.get(java.util.Calendar.DAY_OF_MONTH);
		ltime.hour = cal.get(java.util.Calendar.HOUR_OF_DAY);
		ltime.minute = cal.get(java.util.Calendar.MINUTE);
		ltime.second = cal.get(java.util.Calendar.SECOND);
		ltime.second_part = cal.get(java.util.Calendar.MILLISECOND) * 1000;
		return false;
	}
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncNowLocal(realArgs);
	}
}
