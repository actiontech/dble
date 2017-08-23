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
        MySQLTime lTime1 = new MySQLTime();
        MySQLTime lTime2 = new MySQLTime();
        int lSign = 1;

        nullValue = false;

        if ((args.get(0).isTemporalWithDate() && args.get(1).fieldType() == FieldTypes.MYSQL_TYPE_TIME)
                || (args.get(1).isTemporalWithDate()
                && args.get(0).fieldType() == FieldTypes.MYSQL_TYPE_TIME)) {
            return nullValue = true;
        } // Incompatible types

        if (args.get(0).isTemporalWithDate() || args.get(1).isTemporalWithDate()) {
            if (args.get(0).getDate(lTime1, MyTime.TIME_FUZZY_DATE)
                    || args.get(1).getDate(lTime2, MyTime.TIME_FUZZY_DATE))
                return nullValue = true;
        } else {
            if (args.get(0).getTime(lTime1) || args.get(1).getTime(lTime2))
                return nullValue = true;
        }

        if (lTime1.timeType != lTime2.timeType) {
            return nullValue = true;// Incompatible types
        }
        if (lTime1.neg != lTime2.neg)
            lSign = -lSign;

        MySQLTime lTime3 = new MySQLTime();

        lTime3.neg = MyTime.calc_time_diff(lTime1, lTime2, lSign, seconds, microseconds);

        /*
         * For MYSQL_TIMESTAMP_TIME only: If first argument was negative and
         * diff between arguments is non-zero we need to swap sign to get proper
         * result.
         */
        if (lTime1.neg && (seconds.get() != 0 || microseconds.get() != 0))
            lTime3.neg = lTime3.neg ? false : true; // Swap sign of result

        MyTime.calc_time_from_sec(lTime3, seconds.get(), microseconds.get());
        return false;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncTimediff(realArgs);
    }

}
