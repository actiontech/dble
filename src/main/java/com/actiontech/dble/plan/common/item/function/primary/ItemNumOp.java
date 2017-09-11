/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.primary;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.time.MySQLTime;

import java.util.ArrayList;

/**
 * Base class for operations like '+', '-', '*'
 */
public abstract class ItemNumOp extends ItemFuncNumhybrid {

    public ItemNumOp(Item a, Item b) {
        super(new ArrayList<Item>());
        args.add(a);
        args.add(b);
    }

    /**
     * resultPrecision
     */
    public abstract void resultPrecision();

    @Override
    public void findNumType() {
        ItemResult r0 = args.get(0).numericContextResultType();
        ItemResult r1 = args.get(1).numericContextResultType();

        assert (r0 != ItemResult.STRING_RESULT && r1 != ItemResult.STRING_RESULT);

        if (r0 == ItemResult.REAL_RESULT || r1 == ItemResult.REAL_RESULT) {
            /*
             * Since DATE/TIME/DATETIME data types return
             * INT_RESULT/DECIMAL_RESULT type codes, we should never get to here
             * when both fields are temporal.
             */
            assert (!args.get(0).isTemporal() || !args.get(1).isTemporal());
            countRealLength();
            maxLength = floatLength(decimals);
            hybridType = ItemResult.REAL_RESULT;
        } else if (r0 == ItemResult.DECIMAL_RESULT || r1 == ItemResult.DECIMAL_RESULT) {
            hybridType = ItemResult.DECIMAL_RESULT;
            resultPrecision();
        } else {
            assert (r0 == ItemResult.INT_RESULT && r1 == ItemResult.INT_RESULT);
            decimals = 0;
            hybridType = ItemResult.INT_RESULT;
            resultPrecision();
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
