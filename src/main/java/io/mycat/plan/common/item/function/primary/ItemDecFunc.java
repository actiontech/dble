package io.mycat.plan.common.item.function.primary;

import io.mycat.plan.common.item.Item;

import java.util.List;


public abstract class ItemDecFunc extends ItemRealFunc {

    public ItemDecFunc(List<Item> args) {
        super(args);
    }

    @Override
    public void fixLengthAndDec() {
        decimals = NOT_FIXED_DEC;
        maxLength = floatLength(decimals);
        maybeNull = true;
    }
}
