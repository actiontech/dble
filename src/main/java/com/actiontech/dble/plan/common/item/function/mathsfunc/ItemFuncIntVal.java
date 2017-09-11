/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.primary.ItemFuncNum1;

import java.util.List;


public abstract class ItemFuncIntVal extends ItemFuncNum1 {

    public ItemFuncIntVal(List<Item> args) {
        super(args);
    }

    @Override
    public void fixNumLengthAndDec() {
        decimals = 0;
    }

    @Override
    public void findNumType() {
        ItemResult i = hybridType = args.get(0).resultType();
        if (i == ItemResult.STRING_RESULT || i == ItemResult.REAL_RESULT) {
            hybridType = ItemResult.REAL_RESULT;
            maxLength = floatLength(decimals);

        } else if (i == ItemResult.INT_RESULT) {
            hybridType = ItemResult.INT_RESULT;

        } else if (i == ItemResult.DECIMAL_RESULT) {
            hybridType = ItemResult.DECIMAL_RESULT;

        } else {
            assert (false);
        }
    }

}
