/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.primary;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.time.MySQLTime;

import java.util.List;


/**
 * function where type of result detected by first argument
 */
public abstract class ItemFuncNum1 extends ItemFuncNumhybrid {

    public ItemFuncNum1(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public void fixNumLengthAndDec() {
        decimals = args.get(0).getDecimals();
        this.maxLength = args.get(0).getMaxLength();
    }

    @Override
    public void findNumType() {
        Item.ItemResult i = hybridType = args.get(0).resultType();
        if (i == Item.ItemResult.INT_RESULT) {
            //do nothing
        } else if (i == Item.ItemResult.STRING_RESULT || i == Item.ItemResult.REAL_RESULT) {
            hybridType = Item.ItemResult.REAL_RESULT;
            maxLength = floatLength(decimals);
        } else if (i == Item.ItemResult.DECIMAL_RESULT) {
            //do nothing
        } else {
            assert (false);
        }
    }

    @Override
    public String strOp() {
        return null;
    }

    @Override
    public boolean dateOp(MySQLTime ltime, long flags) {
        return false;
    }

    @Override
    public boolean timeOp(MySQLTime ltime) {
        return false;
    }
}
