package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;
import io.mycat.plan.common.time.MyTime;

import java.math.BigInteger;
import java.util.List;

public class ItemFuncPeriodAdd extends ItemIntFunc {

    public ItemFuncPeriodAdd(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "period_add";
    }

    @Override
    public BigInteger valInt() {
        long period = args.get(0).valInt().longValue();
        long months = args.get(1).valInt().longValue();

        if ((nullValue = args.get(0).isNullValue() || args.get(1).isNullValue()) || period == 0L)
            return BigInteger.ZERO; /* purecov: inspected */
        return BigInteger.valueOf(MyTime.convertMonthToPeriod(MyTime.convertPeriodToMonth(period) + months));
    }

    @Override
    public void fixLengthAndDec() {
        maxLength = 6;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncPeriodAdd(realArgs);
    }
}
