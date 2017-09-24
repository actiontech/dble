/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.timefunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.actiontech.dble.plan.common.time.MyTime;

import java.math.BigDecimal;
import java.util.List;

public class ItemFuncSecToTime extends ItemTimeFunc {

    public ItemFuncSecToTime(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "sec_to_time";
    }

    @Override
    public void fixLengthAndDec() {
        maybeNull = true;
        fixLengthAndDecAndCharsetDatetime(MyTime.MAX_TIME_WIDTH, args.get(0).getDecimals());
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        BigDecimal val = args.get(0).valDecimal();
        if (nullValue = args.get(0).isNullValue()) {
            return true;
        }
        long seconds = val.longValue();
        if (seconds > MyTime.TIME_MAX_SECOND) {
            ltime.setMaxHhmmss();
            return true;
        }
        ltime.setHour((seconds / 3600));
        long sec = (seconds % 3600);
        ltime.setMinute(sec / 60);
        ltime.setSecond(sec % 60);

        long microseconds = (long) ((val.doubleValue() - val.longValue()) * 1000000);
        ltime.setSecondPart(microseconds);
        return false;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncSecToTime(realArgs);
    }
}
