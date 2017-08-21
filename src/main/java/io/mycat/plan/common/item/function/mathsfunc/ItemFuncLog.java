package io.mycat.plan.common.item.function.mathsfunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemDecFunc;

import java.math.BigDecimal;
import java.util.List;


public class ItemFuncLog extends ItemDecFunc {

    public ItemFuncLog(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "log";
    }

    /**
     * Extended but so slower LOG function.
     * <p>
     * We have to check if all values are > zero and first one is not one as
     * these are the cases then result is not a number.
     */
    public BigDecimal valReal() {
        double value = args.get(0).valReal().doubleValue();
        if ((nullValue = args.get(0).nullValue))
            return BigDecimal.ZERO;
        if (value <= 0.0) {
            signalDivideByNull();
            return BigDecimal.ZERO;
        }
        if (args.size() == 2) {
            double value2 = args.get(1).valReal().doubleValue();
            if ((nullValue = args.get(1).nullValue))
                return BigDecimal.ZERO;
            if (value2 <= 0.0 || value == 1.0) {
                signalDivideByNull();
                return BigDecimal.ZERO;
            }
            return new BigDecimal(Math.log(value2) / Math.log(value));
        }
        return new BigDecimal(Math.log(value));
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncLog(realArgs);
    }
}
