package io.mycat.plan.common.item.function.mathsfunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemDecFunc;

import java.math.BigDecimal;
import java.util.List;


public class ItemFuncSin extends ItemDecFunc {

    public ItemFuncSin(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "sin";
    }

    public BigDecimal valReal() {
        double db = args.get(0).valReal().doubleValue();
        if (args.get(0).isNull()) {
            this.nullValue = true;
            return BigDecimal.ZERO;
        } else {
            return new BigDecimal(Math.sin(db));
        }
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncSin(realArgs);
    }
}
