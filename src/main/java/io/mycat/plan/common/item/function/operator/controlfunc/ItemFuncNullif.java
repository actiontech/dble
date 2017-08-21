package io.mycat.plan.common.item.function.operator.controlfunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.operator.ItemBoolFunc2;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;


public class ItemFuncNullif extends ItemBoolFunc2 {
//    ItemResult cached_result_type;//use for agg_arg_charsets_for_comparison

    public ItemFuncNullif(Item a, Item b) {
        super(a, b);
    }

    @Override
    public final String funcName() {
        return "nullif";
    }

    @Override
    public BigDecimal valReal() {
        BigDecimal value;
        if (cmp.compare() == 0) {
            nullValue = true;
            return BigDecimal.ZERO;
        }
        value = args.get(0).valReal();
        nullValue = args.get(0).nullValue;
        return value;
    }

    @Override
    public BigInteger valInt() {
        BigInteger value;
        if (cmp.compare() == 0) {
            nullValue = true;
            return BigInteger.ZERO;
        }
        value = args.get(0).valInt();
        nullValue = args.get(0).nullValue;
        return value;
    }

    @Override
    public String valStr() {
        String res;
        if (cmp.compare() == 0) {
            nullValue = true;
            return null;
        }
        res = args.get(0).valStr();
        nullValue = args.get(0).nullValue;
        return res;
    }

    @Override
    public BigDecimal valDecimal() {
        BigDecimal value;
        if (cmp.compare() == 0) {
            nullValue = true;
            return null;
        }
        value = args.get(0).valDecimal();
        nullValue = args.get(0).nullValue;
        return value;
    }

    @Override
    public boolean isNull() {
        return (nullValue = (cmp.compare() == 0 ? true : args.get(0).nullValue));
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncNullif(realArgs.get(0), realArgs.get(1));
    }
}
