/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.cmpfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.primary.ItemBoolFunc;

import java.math.BigInteger;


public abstract class ItemFuncTruth extends ItemBoolFunc {

    /**
     * True for <code>X IS [NOT] TRUE</code>, false for
     * <code>X IS [NOT] FALSE</code> predicates.
     */
    final boolean value;
    /**
     * True for <code>X IS Y</code>, false for <code>X IS NOT Y</code>
     * predicates.
     */
    final boolean affirmative;

    public ItemFuncTruth(Item a, boolean avalue, boolean aaffirmative) {
        super(a);
        this.value = avalue;
        this.affirmative = aaffirmative;
    }

    @Override
    public boolean valBool() {
        boolean val = args.get(0).valBool();
        if (args.get(0).isNull()) {
            /*
             * NULL val IS {TRUE, FALSE} --> FALSE NULL val IS NOT {TRUE, FALSE}
             * --> TRUE
             */
            return (!affirmative);
        }

        if (affirmative) {
            /* {TRUE, FALSE} val IS {TRUE, FALSE} value */
            return (val == value);
        }

        /* {TRUE, FALSE} val IS NOT {TRUE, FALSE} value */
        return (val != value);
    }

    @Override
    public BigInteger valInt() {
        return (valBool() ? BigInteger.ONE : BigInteger.ZERO);
    }

    @Override
    public void fixLengthAndDec() {
        maybeNull = false;
        nullValue = false;
        decimals = 0;
        maxLength = 1;
    }

}
