package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.time.MySQLTime;

import java.util.List;


public class ItemFuncCurtimeUtc extends ItemTimeFunc {

    public ItemFuncCurtimeUtc(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "utc_time";
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
        java.util.Calendar cal = getUTCTime();
        ltime.setDay(0);
        ltime.setMonth(0);
        ltime.setYear(0);
        ltime.setHour(cal.get(java.util.Calendar.HOUR_OF_DAY));
        ltime.setMinute(cal.get(java.util.Calendar.MINUTE));
        ltime.setSecond(cal.get(java.util.Calendar.SECOND));
        ltime.setSecondPart(cal.get(java.util.Calendar.MILLISECOND) * 1000);
        return false;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncCurtimeUtc(realArgs);
    }
}
