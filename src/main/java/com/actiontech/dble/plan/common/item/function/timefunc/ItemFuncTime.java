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
