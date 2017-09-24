/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.primary;

import com.actiontech.dble.plan.common.item.Item;

import java.util.ArrayList;
import java.util.List;


public abstract class ItemBoolFunc extends ItemIntFunc {

    public ItemBoolFunc(Item a) {
        this(new ArrayList<Item>());
        args.add(a);
    }

    public ItemBoolFunc(Item a, Item b) {
        this(new ArrayList<Item>());
        args.add(a);
        args.add(b);
    }

    public ItemBoolFunc(List<Item> args) {
        super(args);
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
