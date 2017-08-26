package io.mycat.plan.common.item.function.mathsfunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemRealFunc;

import java.math.BigDecimal;
import java.util.List;


public class ItemFuncRand extends ItemRealFunc {
    // 理论上应该是每个连接有一个单独的种子保存起来,我们这里使用一个全局种子模拟下
    // boolean first_eval; // TRUE if val_real() is called 1st time

    public ItemFuncRand(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "rand";
    }

    public BigDecimal valReal() {
        return new BigDecimal(Math.random());
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncRand(realArgs);
    }

}
