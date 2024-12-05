/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.primary;

import com.oceanbase.obsharding_d.plan.common.item.Item;

import java.util.ArrayList;
import java.util.List;


public abstract class ItemBoolFunc extends ItemIntFunc {

    public ItemBoolFunc(Item a, int charsetIndex) {
        this(new ArrayList<>(), charsetIndex);
        args.add(a);
    }

    public ItemBoolFunc(Item a, Item b, int charsetIndex) {
        this(new ArrayList<>(), charsetIndex);
        args.add(a);
        args.add(b);
    }

    public ItemBoolFunc(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public void fixLengthAndDec() {
        decimals = 0;
        maxLength = 1;
    }

    @Override
    public int decimalPrecision() {
        return 1;
    }

}
