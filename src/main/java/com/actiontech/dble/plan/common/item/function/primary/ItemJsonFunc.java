package com.actiontech.dble.plan.common.item.function.primary;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.time.MySQLTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public abstract class ItemJsonFunc extends ItemFunc {
    public ItemJsonFunc(Item a) {
        this(new ArrayList<Item>());
        args.add(a);
    }

    public ItemJsonFunc(Item a, Item b) {
        this(new ArrayList<Item>());
        args.add(a);
        args.add(b);
    }
    public ItemJsonFunc(List<Item> args) {
        super(args);
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
    public String valStr() {
        return null;
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        return false;
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return false;
    }

    @Override
    public Item.ItemResult resultType() {
        return ItemResult.STRING_RESULT;
    }
}
