/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;

import java.util.List;

public class ItemFuncTime extends ItemTimeFunc {

    public ItemFuncTime(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "time";
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return getArg0Time(ltime);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncTime(realArgs);
    }
}
