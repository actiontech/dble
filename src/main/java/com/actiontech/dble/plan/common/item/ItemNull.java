/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class ItemNull extends ItemBasicConstant {
    private void init() {
        maybeNull = nullValue = true;
        maxLength = 0;
        fixed = true;
        itemName = "NULL";
    }

    public ItemNull() {
        init();
    }

    @Override
    public ItemType type() {
        return ItemType.NULL_ITEM;
    }

    @Override
    public int hashCode() {
        return "null".hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        return obj instanceof ItemNull;
    }

    @Override
    public BigDecimal valReal() {
        nullValue = true;
        return BigDecimal.ZERO;
    }

    @Override
    public BigInteger valInt() {
        nullValue = true;
        return BigInteger.ZERO;
    }

    @Override
    public long valTimeTemporal() {
        return valInt().longValue();
    }

    @Override
    public long valDateTemporal() {
        return valInt().longValue();
    }

    @Override
    public String valStr() {
        nullValue = true;
        return null;
    }

    @Override
    public BigDecimal valDecimal() {
        return null;
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        return true;
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return true;
    }

    @Override
    public ItemResult resultType() {
        return ItemResult.STRING_RESULT;
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_NULL;
    }

    @Override
    public boolean isNull() {
        return true;
    }

    @Override
    public SQLExpr toExpression() {
        return new SQLNullExpr();
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        return new ItemNull();
    }
}
