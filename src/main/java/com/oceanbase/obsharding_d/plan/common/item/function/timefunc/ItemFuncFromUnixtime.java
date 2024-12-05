/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.timefunc;

import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.plan.common.time.MySQLTime;

import java.util.Calendar;
import java.util.List;


public class ItemFuncFromUnixtime extends ItemDatetimeFunc {

    public ItemFuncFromUnixtime(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "from_unixtime";
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzyDate) {
        long milseconds = args.get(0).valInt().longValue() * 1000;
        if (nullValue = args.get(0).isNullValue())
            return true;
        java.util.Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(milseconds);
        ltime.setCal(cal);
        return false;
    }

    @Override
    public void fixLengthAndDec() {

    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncFromUnixtime(realArgs, charsetIndex);
    }
}
