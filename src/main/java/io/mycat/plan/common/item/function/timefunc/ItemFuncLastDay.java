package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MySQLTimestampType;
import io.mycat.plan.common.time.MyTime;

import java.util.List;

public class ItemFuncLastDay extends ItemDateFunc {

    public ItemFuncLastDay(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "last_day";
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzy_date) {
        if ((nullValue = getArg0Date(ltime, fuzzy_date)))
            return true;

        if (ltime.month == 0) {
            /*
             * Cannot calculate last day for zero month. Let's print a warning
             * and return NULL.
             */
            ltime.time_type = MySQLTimestampType.MYSQL_TIMESTAMP_DATE;
            return (nullValue = true);
        }

        int monthIdx = (int) ltime.month - 1;
        ltime.day = MyTime.days_in_month[monthIdx];
        if (monthIdx == 1 && MyTime.calc_days_in_year(ltime.year) == 366)
            ltime.day = 29;
        MyTime.datetime_to_date(ltime);
        return false;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncLastDay(realArgs);
    }

}
