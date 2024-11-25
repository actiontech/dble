/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item;

import com.oceanbase.obsharding_d.plan.common.field.Field;
import com.oceanbase.obsharding_d.plan.common.time.MySQLTime;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBooleanExpr;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class ItemBoolean extends ItemBasicConstant {
    private Boolean value;
    private BigInteger intValue;


    public ItemBoolean(Boolean value) {
        this.value = value;
        intValue = BigInteger.valueOf(value ? 1 : 0);
        maxLength = 5;
        fixed = true;
        decimals = NOT_FIXED_DEC;
    }

    @Override
    public ItemType type() {
        return ItemType.BOOLEAN_ITEM;
    }

    @Override
    public ItemResult resultType() {
        return ItemResult.INT_RESULT;
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_LONGLONG;
    }

    @Override
    public BigDecimal valReal() {
        return new BigDecimal(intValue);
    }

    @Override
    public BigInteger valInt() {
        return intValue;
    }

    @Override
    public String valStr() {
        return value.toString();
    }

    @Override
    public BigDecimal valDecimal() {
        return valReal();
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
    public int decimalPrecision() {
        return maxLength - (intValue.signum() == -1 ? 1 : 0);
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
        if (!(obj instanceof ItemBoolean))
            return false;
        ItemBoolean other = (ItemBoolean) obj;
        if (value == null || other.value == null)
            return value == null && other.value == null;
        else
            return value.equals(other.value);
    }

    @Override
    public SQLExpr toExpression() {
        return new SQLBooleanExpr(value);
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        return new ItemBoolean(value);
    }
}
