package io.mycat.plan.common.item;

import io.mycat.plan.common.context.NameResolutionContext;
import io.mycat.plan.common.context.ReferContext;

public abstract class ItemBasicConstant extends Item {

    protected ItemBasicConstant() {
        this.withUnValAble = false;
        this.withIsNull = false;
        this.withSubQuery = false;
        this.withSumFunc = false;
    }

    @Override
    public boolean basicConstItem() {
        return true;
    }

    @Override
    public final Item fixFields(NameResolutionContext context) {
        return this;
    }

    @Override
    public final void fixRefer(ReferContext context) {
        // constant no need to add in refer
    }

}
