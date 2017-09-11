/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;

import java.util.List;


public class ItemFuncDate extends ItemDateFunc {

    public ItemFuncDate(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "date";
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzyDate) {
        if (nullValue = args.get(0).isNullValue()) {
            return true;
        }
        nullValue = getArg0Date(ltime, fuzzyDate);
        return nullValue;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncDate(realArgs);
    }
}
