/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.time.Timeval;

import java.util.List;

public class ItemFuncUnixTimestamp extends ItemTimevalFunc {

    public ItemFuncUnixTimestamp(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "unix_timestamp";
    }

    public void fixLengthAndDec() {
        fixLengthAndDecAndCharsetDatetime(11, getArgCount() == 0 ? 0 : args.get(0).datetimePrecision());
    }

    @Override
    public boolean valTimeval(Timeval tm) {
        if (getArgCount() == 0) {
            tm.setTvSec(java.util.Calendar.getInstance().getTimeInMillis() / 1000);
            tm.setTvUsec(0);
            return false; // no args: null_value is set in constructor and is
            // always 0.
        }
        return (nullValue = args.get(0).getTimeval(tm));
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncUnixTimestamp(realArgs);
    }
}
