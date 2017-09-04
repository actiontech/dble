package com.actiontech.dble.plan.common.item;

public abstract class ItemResultField extends Item {

    protected ItemResultField() {
        this.withUnValAble = true;
    }


    public void cleanup() {

    }

    public abstract void fixLengthAndDec();

    public abstract String funcName();
}
