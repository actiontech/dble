/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class ItemString extends ItemBasicConstant {
    private String value;

    public ItemString(String value, int charsetIndex) {
        this.charsetIndex = charsetIndex;
        this.value = value;
        maxLength = value.length();
        fixed = true;
        decimals = NOT_FIXED_DEC;
    }

    @Override
    public ItemType type() {
        return ItemType.STRING_ITEM;
    }

    @Override
    public BigDecimal valReal() {
        BigDecimal decValue = BigDecimal.ZERO;
        try {
            decValue = new BigDecimal(value);
        } catch (Exception e) {
            LOGGER.info("convert string to decimal exception!", e);
        }
        return decValue;
    }

    @Override
    public BigInteger valInt() {
        BigInteger intValue = BigInteger.ZERO;
        try {
            return new BigInteger(value);
        } catch (Exception e) {
            if (e instanceof NumberFormatException) {
                try {
                    return new BigInteger(value.getBytes());
                } catch (Exception e2) {
                    LOGGER.info("convert string to int exception!", e);
                }
            } else {
                LOGGER.info("convert string to int exception!", e);
            }
        }
        return intValue;
    }

    @Override
    public String valStr() {
        return value;
    }

    @Override
    public BigDecimal valDecimal() {
        return valDecimalFromString();
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        return getDateFromString(ltime, fuzzydate);
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return getTimeFromString(ltime);
    }

    @Override
    public ItemResult resultType() {
        return ItemResult.STRING_RESULT;
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_VARCHAR;
    }

    @Override
    public int hashCode() {
        if (value == null)
            return 0;
        else
            return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof ItemString))
            return false;
        ItemString other = (ItemString) obj;
        if (value == null || other.value == null)
            return value == null && other.value == null;
        else
            return value.equals(other.value);
    }

    @Override
    public SQLExpr toExpression() {
        return new SQLCharExpr(value); //LiteralString(null, value, false);
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        return new ItemString(value, charsetIndex);
    }

}
