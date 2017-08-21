package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.time.MySQLTime;

import java.util.Calendar;
import java.util.List;


public class ItemFuncFromUnixtime extends ItemDatetimeFunc {

    public ItemFuncFromUnixtime(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "from_unixtime";
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzy_date) {
        long milseconds = args.get(0).valInt().longValue() * 1000;
        if (nullValue = args.get(0).nullValue)
            return true;
        java.util.Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(milseconds);
        ltime.setCal(cal);
        return false;
    }

    @Override
    public void fixLengthAndDec() {

    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncFromUnixtime(realArgs);
    }
}
