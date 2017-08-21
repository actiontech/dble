package io.mycat.plan.common.item.function.mathsfunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemDecFunc;

import java.math.BigDecimal;
import java.util.List;


public class ItemFuncPow extends ItemDecFunc {

    public ItemFuncPow(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "pow";
    }

    public BigDecimal valReal() {
        double value = args.get(0).valReal().doubleValue();
        double val2 = args.get(1).valReal().doubleValue();
        if ((nullValue = args.get(0).nullValue || args.get(1).nullValue))
            return BigDecimal.ZERO;
        return new BigDecimal(Math.pow(value, val2));
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncPow(realArgs);
    }
}
