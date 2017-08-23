package io.mycat.plan.common.item.function.primary;

import io.mycat.plan.common.item.Item;

import java.math.BigDecimal;
import java.util.List;


public abstract class ItemFuncUnits extends ItemRealFunc {

    BigDecimal mul, add;

    public ItemFuncUnits(List<Item> args, double mulArg, double addArg) {
        super(args);
        mul = new BigDecimal(mulArg);
        add = new BigDecimal(addArg);
    }

    @Override
    public BigDecimal valReal() {
        BigDecimal value = args.get(0).valReal();
        if ((nullValue = args.get(0).nullValue))
            return BigDecimal.ZERO;
        return value.multiply(mul).add(add);
    }

    @Override
    public void fixLengthAndDec() {
        decimals = NOT_FIXED_DEC;
        maxLength = floatLength(decimals);
    }

}
