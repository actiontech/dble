package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.ptr.LongPtr;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MySQLTimestampType;
import io.mycat.plan.common.time.MyTime;

import java.util.List;

public class ItemFuncMakedate extends ItemDateFunc {

    public ItemFuncMakedate(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "makedate";
    }

    @Override
    /**
     * MAKEDATE(a,b) is a date function that creates a date value from a year
     * and day value.
     *
     * NOTES: As arguments are integers, we can't know if the year is a 2 digit
     * or 4 digit year. In this case we treat all years < 100 as 2 digit years.
     * Ie, this is not safe for dates between 0000-01-01 and 0099-12-31
     */
    public boolean getDate(MySQLTime ltime, long fuzzy_date) {
        long daynr = args.get(1).valInt().longValue();
        long year = args.get(0).valInt().longValue();
        long days;

        if (args.get(0).nullValue || args.get(1).nullValue || year < 0 || year > 9999 || daynr <= 0) {
            nullValue = true;
            return true;
        }

        if (year < 100)
            year = MyTime.year_2000_handling(year);

        days = MyTime.calc_daynr(year, 1, 1) + daynr - 1;
        /* Day number from year 0 to 9999-12-31 */
        if (days >= 0 && days <= MyTime.MAX_DAY_NUMBER) {
            nullValue = false;
            LongPtr lpyear = new LongPtr(0);
            LongPtr lpmonth = new LongPtr(0);
            LongPtr lpday = new LongPtr(0);
            MyTime.get_date_from_daynr(days, lpyear, lpmonth, lpday);
            ltime.year = lpyear.get();
            ltime.month = lpmonth.get();
            ltime.day = lpday.get();
            ltime.neg = false;
            ltime.hour = ltime.minute = ltime.second = ltime.second_part = 0;
            ltime.time_type = MySQLTimestampType.MYSQL_TIMESTAMP_DATE;
            return false;
        }
        nullValue = true;
        return true;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncMakedate(realArgs);
    }

}
