package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;
import io.mycat.plan.common.ptr.LongPtr;
import io.mycat.plan.common.time.MySQLTime;
import io.mycat.plan.common.time.MyTime;

import java.math.BigInteger;
import java.util.List;

public class ItemFuncYearweek extends ItemIntFunc {

    public ItemFuncYearweek(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "yearweek";
    }

    @Override
    public BigInteger valInt() {
        LongPtr year = new LongPtr(0);
        long week;
        MySQLTime ltime = new MySQLTime();
        if (getArg0Date(ltime, MyTime.TIME_NO_ZERO_DATE))
            return BigInteger.ZERO;
        week = MyTime.calc_week(ltime, (MyTime.week_mode(args.size() > 1 ? args.get(1).valInt().intValue() : 0) | MyTime.WEEK_YEAR), year);
        return BigInteger.valueOf(week + year.get() * 100);
    }

    @Override
    public void fixLengthAndDec() {
        fixCharLength(6); /* YYYYWW */
        maybeNull = true;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncYearweek(realArgs);
    }
}
