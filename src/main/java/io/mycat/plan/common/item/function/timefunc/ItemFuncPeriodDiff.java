package io.mycat.plan.common.item.function.timefunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;
import io.mycat.plan.common.time.MyTime;

import java.math.BigInteger;
import java.util.List;

public class ItemFuncPeriodDiff extends ItemIntFunc {

    public ItemFuncPeriodDiff(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "period_diff";
    }

    @Override
    public BigInteger valInt() {
        long period1 = args.get(0).valInt().longValue();
        long period2 = args.get(1).valInt().longValue();

        if ((nullValue = args.get(0).nullValue || args.get(1).nullValue))
            return BigInteger.ZERO; /* purecov: inspected */
        return BigInteger.valueOf(MyTime.convert_period_to_month(period1) - MyTime.convert_period_to_month(period2));
    }

    @Override
    public void fixLengthAndDec() {
        maxLength = 6;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncPeriodDiff(realArgs);
    }

}
