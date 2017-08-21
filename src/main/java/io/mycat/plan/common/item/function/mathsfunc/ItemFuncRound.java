package io.mycat.plan.common.item.function.mathsfunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;

import java.util.List;


public class ItemFuncRound extends ItemFuncRoundOrTruncate {

    public ItemFuncRound(List<Item> args) {
        super(args, false);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncRound(realArgs);
    }

}
