/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.sumfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.time.MySQLTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;


public abstract class ItemSumNum extends ItemSum {

    public ItemSumNum(List<Item> args, boolean isPushDown, List<Field> fields) {
        super(args, isPushDown, fields);
    }

    @Override
    public boolean fixFields() {
        decimals = 0;
        maybeNull = false;
        for (int i = 0; i < getArgCount(); i++) {
            if (!args.get(i).isFixed() && args.get(i).fixFields())
                return true;
            decimals = Math.max(decimals, args.get(i).getDecimals());
            maybeNull |= args.get(i).isMaybeNull();
        }
        maxLength = floatLength(decimals);
        nullValue = true;
        fixLengthAndDec();
        fixed = true;
        return false;
    }

    @Override
    public BigInteger valInt() {
        return valReal().toBigInteger();
    }

    @Override
    public String valStr() {
        return valStringFromReal();
    }

    @Override
    public BigDecimal valDecimal() {
        return valDecimalFromReal();
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        return getDateFromNumeric(ltime, fuzzydate);
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return getTimeFromNumeric(ltime);
    }

}
