/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.mathsfunc;

import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.primary.ItemFuncNum1;

import java.util.List;


public abstract class ItemFuncIntVal extends ItemFuncNum1 {

    public ItemFuncIntVal(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public void fixNumLengthAndDec() {
        decimals = 0;
    }

    @Override
    public void findNumType() {
        ItemResult i = args.get(0).resultType();
        if (i == ItemResult.STRING_RESULT || i == ItemResult.REAL_RESULT) {
            hybridType = ItemResult.REAL_RESULT;
            maxLength = floatLength(decimals);

        } else if (i == ItemResult.INT_RESULT) {
            hybridType = ItemResult.INT_RESULT;

        } else if (i == ItemResult.DECIMAL_RESULT) {
            hybridType = ItemResult.DECIMAL_RESULT;

        } else {
            hybridType = i;
            assert (false);
        }
    }

}
