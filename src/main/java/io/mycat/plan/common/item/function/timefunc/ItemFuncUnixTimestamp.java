package io.mycat.plan.common.item.function.timefunc;

import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.time.Timeval;

public class ItemFuncUnixTimestamp extends ItemTimevalFunc {

	public ItemFuncUnixTimestamp(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "unix_timestamp";
	}

	public void fixLengthAndDec() {
		fixLengthAndDecAndCharsetDatetime(11, getArgCount() == 0 ? 0 : args.get(0).datetimePrecision());
	}

	@Override
	public boolean val_timeval(Timeval tm) {
		if (getArgCount() == 0) {
			tm.tv_sec = java.util.Calendar.getInstance().getTimeInMillis() / 1000;
			tm.tv_usec = 0;
			return false; // no args: null_value is set in constructor and is
							// always 0.
		}
		return (nullValue = args.get(0).getTimeval(tm));
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncUnixTimestamp(realArgs);
	}
}
