/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.primary;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;


public abstract class ItemRealFunc extends ItemFunc {

    public ItemRealFunc(List<Item> args) {
        super(args);
    }

    @Override
    public String valStr() {
        BigDecimal nr = valReal();
        if (nullValue)
            return null;
        return nr.toString();
    }

    @Override
    public BigInteger valInt() {
        return valReal().toBigInteger();
    }

    @Override
    public BigDecimal valDecimal() {
        BigDecimal nr = valReal();
        if (nullValue)
            return null;
        return nr;
    }

    @Override
    public ItemResult resultType() {
        return ItemResult.REAL_RESULT;
    }

    @Override
    public boolean getDate(MySQLTime ltime, long flags) {
        return getDateFromReal(ltime, flags);
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return getTimeFromReal(ltime);
    }

    @Override
    public void fixLengthAndDec() {
        decimals = NOT_FIXED_DEC;
        maxLength = floatLength(decimals);
    }
}
