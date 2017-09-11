/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.util.ArgComparator;
import com.actiontech.dble.plan.common.item.function.primary.ItemBoolFunc;

/**
 * Bool with 2 string args
 */
public abstract class ItemBoolFunc2 extends ItemBoolFunc {
    protected ArgComparator cmp;

    public ItemBoolFunc2(Item a, Item b) {
        super(a, b);
        cmp = new ArgComparator(a, b);
    }

    public int setCmpFunc() {
        return cmp.setCmpFunc(this, args.get(0), args.get(1), true);
    }

    @Override
    public void fixLengthAndDec() {
        maxLength = 1; // Function returns 0 or 1

        /*
         * As some compare functions are generated after sql_yacc, we have to
         * check for out of memory conditions here
         */
        if (args.get(0) == null || args.get(1) == null)
            return;

        /*
         * See agg_item_charsets() in item.cc for comments on character set and
         * collation aggregation.
         */
        ItemResult cmpContext = MySQLcom.itemCmpType(args.get(0).resultType(), args.get(1).resultType());
        args.get(0).setCmpContext(cmpContext);
        args.get(1).setCmpContext(cmpContext);
        // Make a special case of compare with fields to get nicer DATE
        // comparisons

        setCmpFunc();
        return;
    }

    @Override
    public boolean isNull() {
        return args.get(0).isNull() || args.get(1).isNull();
    }
}
