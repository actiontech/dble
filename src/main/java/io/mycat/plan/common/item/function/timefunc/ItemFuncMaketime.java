package io.mycat.plan.common.item.function.timefunc;

import java.math.BigDecimal;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MySQLTimestampType;
import io.mycat.plan.common.time.MyTime;

public class ItemFuncMaketime extends ItemTimeFunc {

	public ItemFuncMaketime(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "maketime";
	}

	@Override
	public void fixLengthAndDec() {
		maybeNull = true;
		fixLengthAndDecAndCharsetDatetime(MyTime.MAX_TIME_WIDTH, args.get(2).decimals);
	}

	/**
	 * MAKETIME(h,m,s) is a time function that calculates a time value from the
	 * total number of hours, minutes, and seconds. Result: Time value
	 */
	@Override
	public boolean getTime(MySQLTime ltime) {
		long hour = args.get(0).valInt().longValue();
		long minute = args.get(1).valInt().longValue();
		BigDecimal sec = args.get(2).valDecimal();
		if ((nullValue = (args.get(0).nullValue || args.get(1).nullValue || args.get(2).nullValue || sec == null
				|| minute < 0 || minute > 59))) {
			return true;
		}
		long scdquot = sec.longValue();
		long scdrem = (long) ((sec.doubleValue() - scdquot) * 1000000);
		if ((nullValue = (scdquot < 0 || scdquot > 59 || scdrem < 0))) {
			return true;
		}

		ltime.set_zero_time(MySQLTimestampType.MYSQL_TIMESTAMP_TIME);

		/* Check for integer overflows */
		if (hour < 0) {
			ltime.neg = true;
		}
		ltime.hour = ((hour < 0 ? -hour : hour));
		ltime.minute = minute;
		ltime.second = scdquot;
		ltime.second_part = scdrem;
		return false;
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncMaketime(realArgs);
	}

}
