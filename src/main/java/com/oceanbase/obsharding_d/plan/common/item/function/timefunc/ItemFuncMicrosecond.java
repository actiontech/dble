/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.timefunc;

import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.plan.common.item.function.primary.ItemIntFunc;
import com.oceanbase.obsharding_d.plan.common.time.MySQLTime;

import java.math.BigInteger;
import java.util.List;

public class ItemFuncMicrosecond extends ItemIntFunc {

    public ItemFuncMicrosecond(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "microsecond";
    }

    @Override
    public BigInteger valInt() {
        MySQLTime ltime = new MySQLTime();
        return getArg0Time(ltime) ? BigInteger.ZERO : BigInteger.valueOf(ltime.getSecondPart());
    }

    @Override
    public void fixLengthAndDec() {
        maybeNull = true;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncMicrosecond(realArgs, charsetIndex);
    }
}
