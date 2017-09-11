/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.sumfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.time.MySQLTime;

import java.math.BigDecimal;
import java.util.List;


public abstract class ItemSumInt extends ItemSumNum {

    public ItemSumInt(List<Item> args, boolean isPushDown, List<Field> fields) {
        super(args, isPushDown, fields);
    }

    @Override
    public BigDecimal valReal() {
        return new BigDecimal(valInt());
    }

    @Override
    public String valStr() {
        return valStringFromInt();
    }

    @Override
    public BigDecimal valDecimal() {
        return valDecimalFromInt();
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        return getDateFromInt(ltime, fuzzydate);
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return getTimeFromInt(ltime);
    }

    @Override
    public final ItemResult resultType() {
        return ItemResult.INT_RESULT;
    }

    @Override
    public void fixLengthAndDec() {
        decimals = 0;
        maxLength = 21;
        maybeNull = nullValue = false;
    }

}
