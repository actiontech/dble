package io.mycat.plan.common.item.function.mathsfunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;

import java.math.BigInteger;
import java.util.List;


public class ItemFuncSign extends ItemIntFunc {

    public ItemFuncSign(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "sign";
    }

    @Override
    public BigInteger valInt() {
        double value = args.get(0).valReal().doubleValue();
        nullValue = args.get(0).nullValue;
        return value < 0.0 ? BigInteger.ONE.negate() : (value > 0 ? BigInteger.ONE : BigInteger.ZERO);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncSign(realArgs);
    }
}
