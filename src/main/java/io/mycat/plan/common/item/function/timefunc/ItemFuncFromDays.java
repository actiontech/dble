package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.ptr.LongPtr;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MySQLTimestampType;
import io.mycat.plan.common.time.MyTime;

import java.util.List;


public class ItemFuncFromDays extends ItemDateFunc {

    public ItemFuncFromDays(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "from_days";
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzyDate) {
        long value = args.get(0).valInt().longValue();
        if ((nullValue = args.get(0).nullValue))
            return true;
        ltime.setZeroTime(ltime.timeType);
        LongPtr lpyear = new LongPtr(0);
        LongPtr lpmonth = new LongPtr(0);
        LongPtr lpday = new LongPtr(0);
        MyTime.getDateFromDaynr((long) value, lpyear, lpmonth, lpday);
        ltime.year = lpyear.get();
        ltime.month = lpmonth.get();
        ltime.day = lpday.get();

        if ((nullValue = ((fuzzyDate & MyTime.TIME_NO_ZERO_DATE) != 0)
                && (ltime.year == 0 || ltime.month == 0 || ltime.day == 0)))
            return true;

        ltime.timeType = MySQLTimestampType.MYSQL_TIMESTAMP_DATE;
        return false;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncFromDays(realArgs);
    }
}
