package io.mycat.plan.common.item.function.timefunc;

import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.strfunc.ItemStrFunc;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;


public class ItemFuncDayname extends ItemStrFunc {

	public ItemFuncDayname(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "dayname";
	}

	@Override
	public String valStr() {
		MySQLTime ltime = new MySQLTime();
		if (getArg0Date(ltime, MyTime.TIME_NO_ZERO_DATE))
			return null;

		long weekday = MyTime.calc_weekday(MyTime.calc_daynr(ltime.year, ltime.month, ltime.day), false);
		return MyTime.day_names[(int) weekday];
	}

	@Override
	public void fixLengthAndDec() {
		maxLength = 9;
		decimals = 0;
		maybeNull = true;
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncDayname(realArgs);
	}

}
