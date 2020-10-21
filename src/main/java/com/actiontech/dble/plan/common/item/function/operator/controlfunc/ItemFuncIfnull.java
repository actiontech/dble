/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.controlfunc;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncCoalesce;
import com.actiontech.dble.plan.common.time.MySQLTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;


public class ItemFuncIfnull extends ItemFuncCoalesce {
    public ItemFuncIfnull(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "ifnull";
    }

    @Override
    public void fixLengthAndDec() {
        hybridType = MySQLcom.aggResultType(args, 0, 2);
        cachedFieldType = MySQLcom.aggFieldType(args, 0, 2);
        maybeNull = args.get(1).isMaybeNull();
        decimals = Math.max(args.get(0).getDecimals(), args.get(1).getDecimals());
    }

    @Override
    public int decimalPrecision() {
        int arg0IntPart = args.get(0).decimalIntPart();
        int arg1IntPart = args.get(1).decimalIntPart();
        int maxIntPart = Math.max(arg0IntPart, arg1IntPart);
        int precision = maxIntPart + decimals;
        return Math.min(precision, MySQLcom.DECIMAL_MAX_PRECISION);
    }

    @Override
    public BigDecimal realOp() {
        BigDecimal value = args.get(0).valReal();
        if (!args.get(0).isNullValue()) {
            nullValue = false;
            return value;
        }
        value = args.get(1).valReal();
        if ((nullValue = args.get(1).isNullValue()))
            return BigDecimal.ZERO;
        return value;
    }

    @Override
    public BigInteger intOp() {
        BigInteger value = args.get(0).valInt();
        if (!args.get(0).isNullValue()) {
            nullValue = false;
            return value;
        }
        value = args.get(1).valInt();
        if ((nullValue = args.get(1).isNullValue()))
            return BigInteger.ZERO;
        return value;
    }

    @Override
    public String strOp() {
        String value = args.get(0).valStr();
        if (!args.get(0).isNullValue()) {
            nullValue = false;
            return value;
        }
        value = args.get(1).valStr();
        if ((nullValue = args.get(1).isNullValue()))
            return null;
        return value;
    }

    @Override
    public BigDecimal decimalOp() {
        BigDecimal value = args.get(0).valDecimal();
        if (!args.get(0).isNullValue()) {
            nullValue = false;
            return value;
        }
        value = args.get(1).valDecimal();
        if ((nullValue = args.get(1).isNullValue()))
            return null;
        return value;
    }

    @Override
    public boolean dateOp(MySQLTime ltime, long fuzzydate) {
        if (!args.get(0).getDate(ltime, fuzzydate))
            return (nullValue = false);
        return (nullValue = args.get(1).getDate(ltime, fuzzydate));
    }

    @Override
    public boolean timeOp(MySQLTime ltime) {
        if (!args.get(0).getTime(ltime))
            return (nullValue = false);
        return (nullValue = args.get(1).getTime(ltime));
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncIfnull(realArgs, charsetIndex);
    }
}
