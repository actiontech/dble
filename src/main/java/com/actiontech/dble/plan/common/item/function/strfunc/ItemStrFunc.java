/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;


public abstract class ItemStrFunc extends ItemFunc {

    protected static final String EMPTY = "";

    public ItemStrFunc(int charsetIndex) {
        this(new ArrayList<Item>(), charsetIndex);
    }

    public ItemStrFunc(Item a, int charsetIndex) {
        this(new ArrayList<Item>(), charsetIndex);
        args.add(a);
    }

    public ItemStrFunc(Item a, Item b, int charsetIndex) {
        this(new ArrayList<Item>(), charsetIndex);
        args.add(a);
        args.add(b);
    }

    public ItemStrFunc(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
        decimals = NOT_FIXED_DEC;
    }

    @Override
    public ItemResult resultType() {
        return ItemResult.STRING_RESULT;
    }

    @Override
    public void fixLengthAndDec() {

    }

    @Override
    public BigDecimal valReal() {
        String res = valStr();
        if (res == null)
            return BigDecimal.ZERO;
        try {
            return new BigDecimal(res);
        } catch (Exception e) {
            return BigDecimal.ZERO;
        }
    }

    @Override
    public BigInteger valInt() {
        String res = valStr();
        if (res == null)
            return BigInteger.ZERO;
        try {
            return new BigInteger(res);
        } catch (Exception e) {
            if (e instanceof NumberFormatException) {
                try {
                    return new BigInteger(res.getBytes());
                } catch (Exception e2) {
                    return BigInteger.ZERO;
                }
            } else {
                return BigInteger.ZERO;
            }
        }
    }

    @Override
    public BigDecimal valDecimal() {
        String res = valStr();
        if (res == null)
            return null;
        try {
            return new BigDecimal(res);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        return getDateFromString(ltime, fuzzydate);
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return getTimeFromString(ltime);
    }

}
