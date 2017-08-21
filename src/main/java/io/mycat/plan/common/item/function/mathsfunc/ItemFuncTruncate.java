package io.mycat.plan.common.item.function.mathsfunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;

import java.util.List;

public class ItemFuncTruncate extends ItemFuncRoundOrTruncate {

    public ItemFuncTruncate(List<Item> args) {
        super(args, true);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncTruncate(realArgs);
    }
}
