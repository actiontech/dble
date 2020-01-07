/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item;

import com.actiontech.dble.plan.common.context.NameResolutionContext;
import com.actiontech.dble.plan.common.context.ReferContext;

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
