package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.ptr.LongPtr;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;

import java.util.List;

public class ItemFuncTimediff extends ItemTimeFunc {

    public ItemFuncTimediff(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "timediff";
    }

    @Override
    public void fixLengthAndDec() {
        fixLengthAndDecAndCharsetDatetime(MyTime.MAX_TIME_WIDTH, 6);
        maybeNull = true;
    }

    /**
     * TIMEDIFF(t,s) is a time function that calculates the time value between a
     * start and end time.
     * <p>
     * t and s: time_or_datetime_expression
     *
     * @param[out] l_time3 Result is stored here.
     * @param[in] flags Not used in this class.
     * @returns
     * @retval false On succes
     * @retval true On error
     */
    @Override
    public boolean getTime(MySQLTime ltime) {
        LongPtr seconds = new LongPtr(0);
        LongPtr microseconds = new LongPtr(0);
        MySQLTime l_time1 = new MySQLTime();
        MySQLTime l_time2 = new MySQLTime();
        int l_sign = 1;

        nullValue = false;

        if ((args.get(0).isTemporalWithDate() && args.get(1).fieldType() == FieldTypes.MYSQL_TYPE_TIME)
                || (args.get(1).isTemporalWithDate()
                && args.get(0).fieldType() == FieldTypes.MYSQL_TYPE_TIME)) {
            return nullValue = true;
        } // Incompatible types

        if (args.get(0).isTemporalWithDate() || args.get(1).isTemporalWithDate()) {
            if (args.get(0).getDate(l_time1, MyTime.TIME_FUZZY_DATE)
                    || args.get(1).getDate(l_time2, MyTime.TIME_FUZZY_DATE))
                return nullValue = true;
        } else {
            if (args.get(0).getTime(l_time1) || args.get(1).getTime(l_time2))
                return nullValue = true;
        }

        if (l_time1.time_type != l_time2.time_type) {
            return nullValue = true;// Incompatible types
        }
        if (l_time1.neg != l_time2.neg)
            l_sign = -l_sign;

        MySQLTime l_time3 = new MySQLTime();

        l_time3.neg = MyTime.calc_time_diff(l_time1, l_time2, l_sign, seconds, microseconds);

		/*
         * For MYSQL_TIMESTAMP_TIME only: If first argument was negative and
		 * diff between arguments is non-zero we need to swap sign to get proper
		 * result.
		 */
        if (l_time1.neg && (seconds.get() != 0 || microseconds.get() != 0))
            l_time3.neg = l_time3.neg ? false : true; // Swap sign of result

        MyTime.calc_time_from_sec(l_time3, seconds.get(), microseconds.get());
        return false;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncTimediff(realArgs);
    }

}
