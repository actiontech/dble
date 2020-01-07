/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

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
