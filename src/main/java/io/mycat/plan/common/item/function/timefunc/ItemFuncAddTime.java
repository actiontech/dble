package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.ptr.LongPtr;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MySQLTimestampType;
import io.mycat.plan.common.time.MyTime;

import java.util.List;

/*
 *  ADDTIME(expr1,expr2)
 *  ADDTIME() adds expr2 to expr1 and returns the result.
 *  expr1 is a time or datetime expression, and expr2 is a time expression.
 */
public class ItemFuncAddTime extends ItemTemporalHybridFunc {

    boolean isDate = false;
    int sign = 1;

    public ItemFuncAddTime(List<Item> args, boolean typeArg, boolean negArg) {
        super(args);
        this.isDate = typeArg;
        this.sign = negArg ? -1 : 1;
    }

    @Override
    public final String funcName() {
        return this.sign == 1 ? "addtime" : "subtime";
    }

    @Override
    public void fixLengthAndDec() {
        /*
         * The field type for the result of an Item_func_add_time function is
         * defined as follows:
         *
         * - If first arg is a MYSQL_TYPE_DATETIME or MYSQL_TYPE_TIMESTAMP
         * result is MYSQL_TYPE_DATETIME - If first arg is a MYSQL_TYPE_TIME
         * result is MYSQL_TYPE_TIME - Otherwise the result is MYSQL_TYPE_STRING
         *
         * TODO: perhaps it should also return MYSQL_TYPE_DATETIME when the
         * first argument is MYSQL_TYPE_DATE.
         */
        if (args.get(0).fieldType() == FieldTypes.MYSQL_TYPE_TIME && !isDate) {
            cached_field_type = FieldTypes.MYSQL_TYPE_TIME;
        } else if (args.get(0).isTemporalWithDateAndTime() || isDate) {
            cached_field_type = FieldTypes.MYSQL_TYPE_DATETIME;
        } else {
            cached_field_type = FieldTypes.MYSQL_TYPE_STRING;
        }
        maybeNull = true;
    }

    @Override
    protected boolean val_datetime(MySQLTime time, long fuzzy_date) {
        MySQLTime l_time1 = new MySQLTime();
        MySQLTime l_time2 = new MySQLTime();
        boolean is_time = false;
        long days;
        LongPtr seconds = new LongPtr(0);
        LongPtr microseconds = new LongPtr(0);
        int l_sign = sign;

        nullValue = false;
        if (cached_field_type == FieldTypes.MYSQL_TYPE_DATETIME) // TIMESTAMP
        // function
        {
            if (getArg0Date(l_time1, fuzzy_date) || args.get(1).getTime(l_time2)
                    || l_time1.time_type == MySQLTimestampType.MYSQL_TIMESTAMP_TIME
                    || l_time2.time_type != MySQLTimestampType.MYSQL_TIMESTAMP_TIME) {
                nullValue = true;
                return true;
            }
        } else // ADDTIME function
        {
            if (args.get(0).getTime(l_time1) || args.get(1).getTime(l_time2)
                    || l_time2.time_type == MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME) {
                nullValue = true;
                return true;
            }
            is_time = (l_time1.time_type == MySQLTimestampType.MYSQL_TIMESTAMP_TIME);
        }
        if (l_time1.neg != l_time2.neg)
            l_sign = -l_sign;

        time.set_zero_time(time.time_type);

        time.neg = MyTime.calc_time_diff(l_time1, l_time2, -l_sign, seconds, microseconds);

        /*
         * If first argument was negative and diff between arguments is non-zero
         * we need to swap sign to get proper result.
         */
        if (l_time1.neg && (seconds.get() != 0 || microseconds.get() != 0))
            time.neg = time.neg ? false : true; // Swap sign of result

        if (!is_time && time.neg) {
            nullValue = true;
            return true;
        }

        days = (long) (seconds.get() / MyTime.SECONDS_IN_24H);

        MyTime.calc_time_from_sec(time, seconds.get() % MyTime.SECONDS_IN_24H, microseconds.get());

        if (!is_time) {
            LongPtr lpyear = new LongPtr(0);
            LongPtr lpmonth = new LongPtr(0);
            LongPtr lpday = new LongPtr(0);
            MyTime.get_date_from_daynr(days, lpyear, lpmonth, lpday);
            time.year = lpyear.get();
            time.month = lpmonth.get();
            time.day = lpday.get();
            time.time_type = MySQLTimestampType.MYSQL_TIMESTAMP_DATETIME;
            if (time.day != 0)
                return false;
            nullValue = true;
            return true;
        }
        time.time_type = MySQLTimestampType.MYSQL_TIMESTAMP_TIME;
        time.hour += days * 24;
        return false;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncAddTime(realArgs, isDate, sign == -1 ? true : false);
    }
}
